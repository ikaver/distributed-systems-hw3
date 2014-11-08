package com.ikaver.aagarwal.hw3.mrmaster.jobmanager;

import java.rmi.RemoteException;
import java.util.List;

import com.google.inject.Inject;
import com.ikaver.aagarwal.hw3.common.config.JobConfig;
import com.ikaver.aagarwal.hw3.common.config.JobInfoForClient;
import com.ikaver.aagarwal.hw3.common.master.IJobManager;
import com.ikaver.aagarwal.hw3.mrmaster.scheduler.IMRScheduler;

public class JobManagerImpl implements IJobManager {
  
  private IMRScheduler scheduler;
  private JobsState jobsState;
  
  @Inject
  public JobManagerImpl(IMRScheduler scheduler, JobsState state) {
    this.scheduler = scheduler;
    this.jobsState = state;
  }
  
  public JobInfoForClient createJob(JobConfig job) throws RemoteException {
    // TODO Auto-generated method stub
    return null;
  }

  public List<JobInfoForClient> listJobs() throws RemoteException {
    // TODO Auto-generated method stub
    return null;
  }

  public boolean terminate(int jobID) throws RemoteException {
    // TODO Auto-generated method stub
    return false;
  }

  public JobInfoForClient getJobInfo(int jobID) throws RemoteException {
    // TODO Auto-generated method stub
    return null;
  }

}
