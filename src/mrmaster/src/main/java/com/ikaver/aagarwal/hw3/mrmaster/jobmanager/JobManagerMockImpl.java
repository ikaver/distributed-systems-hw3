package com.ikaver.aagarwal.hw3.mrmaster.jobmanager;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ikaver.aagarwal.hw3.common.config.FinishedJob;
import com.ikaver.aagarwal.hw3.common.config.JobConfig;
import com.ikaver.aagarwal.hw3.common.config.JobInfoForClient;
import com.ikaver.aagarwal.hw3.common.master.IJobManager;

public class JobManagerMockImpl extends UnicastRemoteObject implements IJobManager {

  private static final long serialVersionUID = 1419606847713584442L;
  private Map<Integer, JobInfoForClient> jobIDToJobInfo;
  private int currentId;
  
  public JobManagerMockImpl() throws RemoteException {
    super();
    this.jobIDToJobInfo = new HashMap<Integer, JobInfoForClient>();
    this.currentId = 0;
  }

  public JobInfoForClient createJob(JobConfig job) throws RemoteException {
    JobInfoForClient jobInfo = new JobInfoForClient(this.getNewID(), job);
    jobIDToJobInfo.put(jobInfo.getJobID(), jobInfo);
    return jobInfo;
  }

  public List<JobInfoForClient> listJobs() throws RemoteException {
    return new ArrayList<JobInfoForClient>(this.jobIDToJobInfo.values());
  }

  public boolean terminate(int jobID) throws RemoteException {
    boolean success = this.jobIDToJobInfo.containsKey(jobID);
    this.jobIDToJobInfo.remove(jobID);
    return success;
  }

  public JobInfoForClient getJobInfo(int jobID) throws RemoteException {
    return this.jobIDToJobInfo.get(jobID);
  }

  private synchronized int getNewID() {
    return this.currentId++;
  }

  public List<FinishedJob> finishedJobs() throws RemoteException {
    return new ArrayList<FinishedJob>();
  }
  
}
