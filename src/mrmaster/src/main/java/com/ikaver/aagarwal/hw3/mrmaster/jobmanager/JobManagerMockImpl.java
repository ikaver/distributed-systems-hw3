package com.ikaver.aagarwal.hw3.mrmaster.jobmanager;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ikaver.aagarwal.hw3.common.config.Job;
import com.ikaver.aagarwal.hw3.common.config.JobInfo;
import com.ikaver.aagarwal.hw3.common.master.IJobManager;

public class JobManagerMockImpl implements IJobManager, Serializable {
  
  private static final long serialVersionUID = 4767647085934965467L;
  private Map<Integer, JobInfo> jobIDToJobInfo;
  private int currentId;
  
  public JobManagerMockImpl() {
    this.jobIDToJobInfo = new HashMap<Integer, JobInfo>();
    this.currentId = 0;
  }

  public JobInfo createJob(Job job) throws RemoteException {
    JobInfo jobInfo = new JobInfo(this.getNewID(), job);
    jobIDToJobInfo.put(jobInfo.getJobID(), jobInfo);
    return jobInfo;
  }

  public List<JobInfo> listJobs() throws RemoteException {
    return new ArrayList<JobInfo>(this.jobIDToJobInfo.values());
  }

  public boolean terminate(int jobID) throws RemoteException {
    boolean success = this.jobIDToJobInfo.containsKey(jobID);
    this.jobIDToJobInfo.remove(jobID);
    return success;
  }

  public JobInfo getJobInfo(int jobID) throws RemoteException {
    return this.jobIDToJobInfo.get(jobID);
  }

  private synchronized int getNewID() {
    return this.currentId++;
  }
  
}
