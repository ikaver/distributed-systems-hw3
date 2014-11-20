package com.ikaver.aagarwal.hw3.mrmaster.jobmanager;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.ikaver.aagarwal.hw3.common.config.FinishedJob;
import com.ikaver.aagarwal.hw3.common.config.JobConfig;
import com.ikaver.aagarwal.hw3.common.config.JobInfoForClient;
import com.ikaver.aagarwal.hw3.common.definitions.Definitions;
import com.ikaver.aagarwal.hw3.common.dfs.FileMetadata;
import com.ikaver.aagarwal.hw3.common.dfs.FileUtil;
import com.ikaver.aagarwal.hw3.common.dfs.IDFS;
import com.ikaver.aagarwal.hw3.common.master.IJobManager;
import com.ikaver.aagarwal.hw3.common.nodemanager.IMRNodeManager;
import com.ikaver.aagarwal.hw3.common.nodemanager.NodeManagerFactory;
import com.ikaver.aagarwal.hw3.common.util.SocketAddress;
import com.ikaver.aagarwal.hw3.common.workers.MapWorkDescription;
import com.ikaver.aagarwal.hw3.common.workers.MapperChunk;
import com.ikaver.aagarwal.hw3.common.workers.ReduceWorkDescription;
import com.ikaver.aagarwal.hw3.mrmaster.jobmanager.jobvalidator.IJobValidator;
import com.ikaver.aagarwal.hw3.mrmaster.jobtracker.IOnWorkCompletedHandler;
import com.ikaver.aagarwal.hw3.mrmaster.jobtracker.IOnWorkerFailedHandler;
import com.ikaver.aagarwal.hw3.mrmaster.jobtracker.JobTracker;
import com.ikaver.aagarwal.hw3.mrmaster.scheduler.IMRScheduler;
import com.ikaver.aagarwal.hw3.mrmaster.scheduler.MapperWorkerInfo;
import com.ikaver.aagarwal.hw3.mrmaster.scheduler.ReducerWorkerInfo;

public class JobManagerImpl extends UnicastRemoteObject implements IJobManager, IOnWorkerFailedHandler,
IOnWorkCompletedHandler {

  private static final long serialVersionUID = -6574342468216313714L;

  private static final Logger LOG = Logger.getLogger(JobManagerImpl.class);

  private Set<SocketAddress> nodeManagers;
  private IMRScheduler       scheduler;
  private IDFS               dfs;
  private IJobValidator      jobValidator;
  private JobsState          jobsState;
  private int                currentJobID;

  @Inject
  public JobManagerImpl(
      IMRScheduler scheduler,
      JobsState state,
      IDFS dfs,
      IJobValidator validator,
      @Named(Definitions.NODE_MANAGER_SET_ANNOTATION) Set<SocketAddress> nodeManagers) throws RemoteException {
    super();
    this.scheduler = scheduler;
    this.jobsState = state;
    this.dfs = dfs;
    this.jobValidator = validator;
    this.nodeManagers = nodeManagers;
  }

  public JobInfoForClient createJob(JobConfig job) throws RemoteException {
    LOG.info("Received a job: " + job);

    if (job == null || !jobValidator.isJobValid(job)) {
      LOG.info("Received invalid job: " + job + " Ignoring...");
      return null;
    }

    LOG.info("Asking DFS for input size of metadata");
    FileMetadata metadata = this.dfs.getMetadata(job.getInputFilePath());
    if(metadata == null) {
      LOG.info("Received job with no input file. Ignoring...");
      return null;
    }

    long sizeOfInputFile = metadata.getSizeOfFile();
    if (sizeOfInputFile <= 0) { 
      LOG.info("Received job with input file size <= 0. Ignoring...");
      return null;
    }

    LOG.info("Got metadata: " + metadata);
    int numMappers = metadata.getNumChunks();

    int jobID = this.getNewJobID();
    // create mappers work descriptions
    Set<MapWorkDescription> mappers = new HashSet<MapWorkDescription>();
    List<MapperChunk> chunks = new ArrayList<MapperChunk>();
    for (int i = 0; i < numMappers; ++i) {
      MapWorkDescription work = new MapWorkDescription(
          jobID, 
          new MapperChunk(
              job.getInputFilePath(),  //input file
              i,                       //partition id
              0,                       //start record
              job.getRecordSize(),     //record size
              (int)FileUtil.getTotalRecords(job.getRecordSize(), metadata.getSizeOfFile())), //records in chunk, 
              job.getJarFile(), 
              job.getMapperClass()
          );
      chunks.add(work.getChunk());
      mappers.add(work);
    }
    LOG.info("Asking scheduler to schedule mappers");
    // schedule the mappers
    Set<MapperWorkerInfo> mapWorkers = scheduler.runMappersForWork(mappers);
    LOG.info("Got mapper workers for job: " + mapWorkers.size());

    // create reducers
    List<SocketAddress> mapperAddresses = new ArrayList<SocketAddress>();
    List<MapWorkDescription> mapperList = new ArrayList<MapWorkDescription>();
    Set<ReduceWorkDescription> reducers = new HashSet<ReduceWorkDescription>();
    for (MapperWorkerInfo mapWorker : mapWorkers) {
      LOG.info(String.format("Got mapper address for job %d: %s", jobID, 
          mapWorker.getNodeManagerAddress()));
      mapperList.add(mapWorker.getWorkDescription());
      mapperAddresses.add(mapWorker.getNodeManagerAddress());
    }
    for (int i = 0; i < job.getNumReducers(); ++i) {
      ReduceWorkDescription work = new ReduceWorkDescription(
          jobID, 
          i,
          job.getNumReducers(),
          job.getReducerClass(),
          mapperList,
          mapperAddresses, /* input sources, socket addresses of mappers */
          job.getOutputFilePath(),
          job.getJarFile()
          );
      reducers.add(work);
    }
    LOG.info("Will ask scheduler to schedule reduces for job " + jobID);
    // schedule the reducers
    Set<ReducerWorkerInfo> reduceWorkers = scheduler
        .runReducersForWork(reducers);
    for(ReducerWorkerInfo workerInfo : reduceWorkers) {
      LOG.info(String.format("Got reducer address for job %d: %s", jobID, 
          workerInfo.getNodeManagerAddress()));
    }

    // create the running job
    ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    RunningJob runningJob = new RunningJob(jobID, job.getJobName(), scheduler);
    JobTracker tracker = new JobTracker(runningJob, this, this);
    scheduler.scheduleAtFixedRate(tracker, 0,
        Definitions.TIME_TO_CHECK_FOR_NODE_MANAGER_STATE, TimeUnit.SECONDS);
    runningJob.getMappers().addAll(mapWorkers);
    runningJob.getReducers().addAll(reduceWorkers);
    jobsState.addJob(runningJob);
    LOG.info("Created job tracker for job with ID: " + runningJob.getJobID());
    return getJobInfo(jobID);
  }

  public List<JobInfoForClient> listJobs() throws RemoteException {
    List<RunningJob> jobs = jobsState.currentlyRunningJobs();
    List<JobInfoForClient> jobsInfo = new ArrayList<JobInfoForClient>();
    for (RunningJob job : jobs) {
      JobInfoForClient jobInfo = jobInfoForClientFromRunningJob(job);
      jobsInfo.add(jobInfo);
    }
    return jobsInfo;
  }


  public List<FinishedJob> finishedJobs() throws RemoteException {
    List<FinishedJob> jobs = jobsState.finishedJobs();
    return jobs;
  }

  public boolean terminate(int jobID) throws RemoteException {
    boolean success = false;
    if (jobsState.getJob(jobID) != null) {
      for (SocketAddress addr : nodeManagers) {
        IMRNodeManager nm = NodeManagerFactory
            .nodeManagerFromSocketAddress(addr);
        if (nm != null) {
          try {
            success = nm.terminateWorkers(jobID) || success;
          }
          catch(RemoteException e) {
            LOG.warn("Failed to communicate with NM " + addr, e);
          }
        }
      }
    }
    return success;
  }

  public void shutdown() throws RemoteException {
    List<RunningJob> runningJobs = jobsState.currentlyRunningJobs();
    for(RunningJob job : runningJobs) {
      terminate(job.getJobID());
    }
    for(SocketAddress addr : nodeManagers) {
      IMRNodeManager nm = NodeManagerFactory
          .nodeManagerFromSocketAddress(addr);
      if (nm != null) {
        try {
          nm.shutdown();
        }
        catch(RemoteException e) {
        }
      }
    }
    System.exit(0);
  }

  public JobInfoForClient getJobInfo(int jobID) throws RemoteException {
    JobInfoForClient info = null;
    RunningJob job = jobsState.getJob(jobID);
    if (job != null) {
      info = jobInfoForClientFromRunningJob(job);
    }
    return info;
  }

  private JobInfoForClient jobInfoForClientFromRunningJob(RunningJob job) {
    return new JobInfoForClient(job.getJobID(), job.getJobName(),
        job.getAmountOfMappers(), job.getAmountOfReducers(),
        job.getAmountOfFinishedMappers(), job.getAmountOfFinishedReducers());
  }

  private int getNewJobID() {
    return ++this.currentJobID;
  }

  /*
   * IOnWorkCompletedHandler methods
   */

  public void onMapperFinished(RunningJob job, MapperWorkerInfo info) {
    LOG.info(String.format("Mapper %s finished for job: %d", 
        info.getNodeManagerAddress(), job.getJobID()));
    job.getFinishedMappers().add(info);
  }

  public void onReducerFinished(RunningJob job, ReducerWorkerInfo info) {
    LOG.info(String.format("Reducer %s finished for job: %d", 
        info.getNodeManagerAddress(), job.getJobID()));
    job.getFinishedReducers().add(info);
  }

  public void onAllMappersFinished(RunningJob job) {
    //nothing to do here
    LOG.info(String.format("All mappers finished for job: " + job.getJobID()));
  }

  public void onAllReducersFinished(RunningJob job) {
    LOG.info(String.format("All reducers finished for job: " + job.getJobID()));
    job.shutdown();
    for(MapperWorkerInfo workerInfo : job.getMappers()) {
      SocketAddress nmAddr = workerInfo.getNodeManagerAddress();
      IMRNodeManager nm = NodeManagerFactory.nodeManagerFromSocketAddress(nmAddr);
      if(nm != null) {
        try {
          nm.terminateWorkers(job.getJobID());
        } catch (RemoteException e) {
          LOG.warn("Failed to communicate with NM", e);
        }
      }
    }
    for(ReducerWorkerInfo workerInfo : job.getReducers()) {
      SocketAddress nmAddr = workerInfo.getNodeManagerAddress();
      IMRNodeManager nm = NodeManagerFactory.nodeManagerFromSocketAddress(nmAddr);
      if(nm != null) {
        try {
          nm.terminateWorkers(job.getJobID());
        } catch (RemoteException e) {
          LOG.warn("Failed to communicate with NM", e);
        }
      }
    }
    this.jobsState.onJobFinished(job.getJobID(), true);
  }

  /*
   * IOnWorkerFailedHandler methods
   */

  public void onMapperFailed(RunningJob job, MapperWorkerInfo info) {
    LOG.info(String.format("Mapper with ID %d for job %d failed. Num failures: %d", 
        info.getWorkDescription().getChunk().getPartitionID(), job.getJobID(), 
        job.getNumFailures()));
    job.setNumFailures(job.getNumFailures()+1);
    if(job.getNumFailures() >= Definitions.MAX_WORKER_RETRIES_BEFORE_CANCELLING_JOB) {
      this.onJobFailed(job);
    }
    else {
      LOG.info(String.format("Mapper %s for job %d failed", 
          info.getNodeManagerAddress(), job.getJobID()));
      HashSet<MapWorkDescription> workSet = new HashSet<MapWorkDescription>();
      workSet.add(info.getWorkDescription());
      Set<MapperWorkerInfo> newInfoSet = scheduler.runMappersForWork(workSet);
      for(MapperWorkerInfo newInfo : newInfoSet) {
        info.setState(newInfo.getState());
        info.setNodeManagerAddress(newInfo.getNodeManagerAddress());
        LOG.info(String.format("Created new mapper %s for job %d",
            info.getNodeManagerAddress(), job.getJobID()));
      }
    }
  }

  public void onReducerFailed(RunningJob job, ReducerWorkerInfo info) {
    LOG.info(String.format("Reducer with ID %d for job %d failed. Num failures: %d", 
        info.getWorkDescription().getReducerID(), job.getJobID(), 
        job.getNumFailures()));
    job.setNumFailures(job.getNumFailures()+1);
    if(job.getNumFailures() >= Definitions.MAX_WORKER_RETRIES_BEFORE_CANCELLING_JOB) {
      this.onJobFailed(job);
    }
    else {
      HashSet<ReduceWorkDescription> workSet = new HashSet<ReduceWorkDescription>();
      workSet.add(info.getWorkDescription());
      Set<ReducerWorkerInfo> newInfoSet = scheduler.runReducersForWork(workSet);
      for(ReducerWorkerInfo newInfo : newInfoSet) {
        info.setState(newInfo.getState());
        info.setNodeManagerAddress(newInfo.getNodeManagerAddress());
        LOG.info(String.format("Created new reducer %s for job %d",
            info.getNodeManagerAddress(), job.getJobID()));
      }
    }
  }

  private void onJobFailed(RunningJob job) {
    if(this.jobsState.getJob(job.getJobID()) != null) {
      LOG.info("Job: " + job.getJobID() + " failed too many times. It will be terminated.");
      this.jobsState.onJobFinished(job.getJobID(), false);
      job.shutdown();
    }
    else {
      LOG.info("Already terminated job with ID: " + job.getJobID());
    }
  }

}
