package com.ikaver.aagarwal.hw3.mrmaster.jobmanager;

import java.rmi.RemoteException;
import java.util.List;

import com.ikaver.aagarwal.hw3.common.config.Job;
import com.ikaver.aagarwal.hw3.common.config.JobInfo;
import com.ikaver.aagarwal.hw3.common.master.IJobManager;

public class JobManagerImpl implements IJobManager {
  
  private JobsState jobsState;

  public JobInfo createJob(Job job) throws RemoteException {
    // TODO Auto-generated method stub
    return null;
  }

  public List<JobInfo> listJobs() throws RemoteException {
    // TODO Auto-generated method stub
    return null;
  }

  public boolean terminate(int jobID) throws RemoteException {
    // TODO Auto-generated method stub
    return false;
  }

  public JobInfo getJobInfo(int jobID) throws RemoteException {
    // TODO Auto-generated method stub
    return null;
  }

}
