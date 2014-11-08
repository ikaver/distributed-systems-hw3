package com.ikaver.aagarwal.hw3.mrmaster.jobmanager;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.ikaver.aagarwal.hw3.common.config.JobConfig;
import com.ikaver.aagarwal.hw3.common.config.JobInfoForClient;
import com.ikaver.aagarwal.hw3.common.definitions.Definitions;
import com.ikaver.aagarwal.hw3.common.master.IJobManager;
import com.ikaver.aagarwal.hw3.common.nodemanager.IMRNodeManager;
import com.ikaver.aagarwal.hw3.common.util.SocketAddress;
import com.ikaver.aagarwal.hw3.mrmaster.scheduler.IMRScheduler;
import com.ikaver.aagarwal.hw3.mrmaster.scheduler.NodeManagerFactory;

public class JobManagerImpl implements IJobManager {
  
  private Set<SocketAddress> nodeManagers;
  private IMRScheduler scheduler;
  private JobsState jobsState;
  
  @Inject
  public JobManagerImpl(IMRScheduler scheduler, JobsState state,
      @Named(Definitions.NODE_MANAGER_SET_ANNOTATION) Set<SocketAddress> nodeManagers) {
    this.scheduler = scheduler;
    this.jobsState = state;
    this.nodeManagers = nodeManagers;
  }
  
  public JobInfoForClient createJob(JobConfig job) throws RemoteException {
    // TODO Auto-generated method stub
    return null;
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

}
