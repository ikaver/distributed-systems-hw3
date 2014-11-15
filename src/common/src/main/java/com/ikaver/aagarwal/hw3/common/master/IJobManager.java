package com.ikaver.aagarwal.hw3.common.master;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

import com.ikaver.aagarwal.hw3.common.config.FinishedJob;
import com.ikaver.aagarwal.hw3.common.config.JobConfig;
import com.ikaver.aagarwal.hw3.common.config.JobInfoForClient;

/***
 * Job Manager class. Object that lives in the master node that is responsible
 * of managing the MR jobs currently running on the system.
 */
public interface IJobManager extends Remote {

  public JobInfoForClient createJob(JobConfig job) throws RemoteException;
  public List<JobInfoForClient> listJobs() throws RemoteException;
  public boolean terminate(int jobID) throws RemoteException;
  public JobInfoForClient getJobInfo(int jobID) throws RemoteException;
  public List<FinishedJob> finishedJobs() throws RemoteException;
  
}
