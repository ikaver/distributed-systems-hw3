package com.ikaver.aagarwal.hw3.mrmaster.jobmanager;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.ikaver.aagarwal.hw3.common.config.JobConfig;
import com.ikaver.aagarwal.hw3.common.config.JobInfoForClient;
import com.ikaver.aagarwal.hw3.common.definitions.Definitions;
import com.ikaver.aagarwal.hw3.common.dfs.IDFS;
import com.ikaver.aagarwal.hw3.common.master.IJobManager;
import com.ikaver.aagarwal.hw3.common.nodemanager.IMRNodeManager;
import com.ikaver.aagarwal.hw3.common.util.SocketAddress;
import com.ikaver.aagarwal.hw3.common.workers.MapWorkDescription;
import com.ikaver.aagarwal.hw3.common.workers.MapperChunk;
import com.ikaver.aagarwal.hw3.common.workers.ReduceWorkDescription;
import com.ikaver.aagarwal.hw3.mrmaster.jobmanager.jobvalidator.IJobValidator;
import com.ikaver.aagarwal.hw3.mrmaster.scheduler.IMRScheduler;
import com.ikaver.aagarwal.hw3.mrmaster.scheduler.MapperWorkerInfo;
import com.ikaver.aagarwal.hw3.mrmaster.scheduler.NodeManagerFactory;
import com.ikaver.aagarwal.hw3.mrmaster.scheduler.ReducerWorkerInfo;

public class JobManagerImpl implements IJobManager {
  
  private Set<SocketAddress> nodeManagers;
  private IMRScheduler scheduler;
  private IDFS dfs;
  private IJobValidator jobValidator;
  private JobsState jobsState;
  private int currentJobID;
  
  @Inject
  public JobManagerImpl(IMRScheduler scheduler, JobsState state, IDFS dfs,
      IJobValidator validator,
      @Named(Definitions.NODE_MANAGER_SET_ANNOTATION) Set<SocketAddress> nodeManagers) {
    this.scheduler = scheduler;
    this.jobsState = state;
    this.dfs = dfs;
    this.jobValidator = validator;
    this.nodeManagers = nodeManagers;
  }
  
  public JobInfoForClient createJob(JobConfig job) throws RemoteException {
    if(job == null || !jobValidator.isJobValid(job)) return null;
    
    long sizeOfInputFile = this.dfs.sizeOfFileInBytes(job.getInputFilePath());
    if(sizeOfInputFile < 0) return null;
    int numberOfRecordsInFile = (int) Math.ceil(sizeOfInputFile / (double)job.getRecordSize());
    int numberOfRecordsPerMapper = (int) Math.ceil(numberOfRecordsInFile / (double)job.getNumMappers());
    int currentStartRecord = 0;
    int jobID = this.getNewJobID();
    Set<MapWorkDescription> mappers = new HashSet<MapWorkDescription>();
    Set<ReduceWorkDescription> reducers = new HashSet<ReduceWorkDescription>();
    List<MapperChunk> chunks = new ArrayList<MapperChunk>();
    for(int i = 0; i < job.getNumMappers(); ++i) {
      MapWorkDescription work = new MapWorkDescription(
          jobID,
          new MapperChunk(
              job.getInputFilePath(), 
              i, 
              currentStartRecord, 
              job.getRecordSize(),
              numberOfRecordsPerMapper),
          job.getJarFilePath(), 
          job.getMapperClass()
      );
      chunks.add(work.getChunk());
      mappers.add(work);
      currentStartRecord += numberOfRecordsPerMapper;
    }
    Set<MapperWorkerInfo> mapWorkers = scheduler.runMappersForWork(mappers);
    List<SocketAddress> mapperAddresses = new ArrayList<SocketAddress>();
    for(MapperWorkerInfo mapWorker : mapWorkers) {
      mapperAddresses.add(mapWorker.getNodeManagerAddress());
    }
    for(int i = 0; i < job.getNumReducers(); ++i) {
      ReduceWorkDescription work = new ReduceWorkDescription(
          jobID, 
          i, 
          mapperAddresses, /* input sources, socket addresses of mappers */
          chunks, /* Mapper chunks */
          job.getOutputFilePath()
      );
      reducers.add(work);
    }
    Set<ReducerWorkerInfo> reduceWorkers = scheduler.runReducersForWork(reducers);
    RunningJob runningJob = new RunningJob(jobID, job.getJobName());
    runningJob.getMappers().addAll(mapWorkers);
    runningJob.getReducers().addAll(reduceWorkers);
    jobsState.addJob(runningJob);
    return getJobInfo(jobID);
  }

  public List<JobInfoForClient> listJobs() throws RemoteException {
    List<RunningJob> jobs = jobsState.currentlyRunningJobs();
    List<JobInfoForClient> jobsInfo = new ArrayList<JobInfoForClient>();
    for(RunningJob job : jobs) {
      JobInfoForClient jobInfo = jobInfoForClientFromRunningJob(job);
      jobsInfo.add(jobInfo);
    }
    return jobsInfo;
  }

  public boolean terminate(int jobID) throws RemoteException {
    boolean success = false;
    if(jobsState.getJob(jobID) != null) {
      for(SocketAddress addr : nodeManagers) {
        IMRNodeManager nm = NodeManagerFactory.nodeManagerFromSocketAddress(addr);
        if(nm != null) {
          success = nm.terminateWorkers(jobID) || success;
        }
      }
    }
    return success;
  }

  public JobInfoForClient getJobInfo(int jobID) throws RemoteException {
    JobInfoForClient info = null;
    RunningJob job = jobsState.getJob(jobID);
    if(job != null) {
      info = jobInfoForClientFromRunningJob(job);
    }
    return info;
  }
  
  private JobInfoForClient jobInfoForClientFromRunningJob(RunningJob job) {
    return new JobInfoForClient(
        job.getJobID(),
        job.getJobName(),
        job.getAmountOfMappers(),
        job.getAmountOfReducers(),
        job.getAmountOfFinishedMappers(),
        job.getAmountOfFinishedReducers()
    );
  }

  private int getNewJobID() {
    return ++this.currentJobID;
  }
  
}
