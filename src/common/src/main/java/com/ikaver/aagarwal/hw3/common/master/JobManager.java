package com.ikaver.aagarwal.hw3.common.master;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

import com.ikaver.aagarwal.hw3.common.config.Job;
import com.ikaver.aagarwal.hw3.common.config.JobInfo;

public interface JobManager extends Remote {

  public JobInfo createJob(Job job) throws RemoteException;
  public List<JobInfo> listJobs() throws RemoteException;
  public boolean terminate(int jobID) throws RemoteException;
  public JobInfo getJobInfo(int jobID) throws RemoteException;
  
}
