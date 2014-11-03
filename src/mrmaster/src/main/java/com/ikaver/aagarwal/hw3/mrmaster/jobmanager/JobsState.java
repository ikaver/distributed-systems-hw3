package com.ikaver.aagarwal.hw3.mrmaster.jobmanager;

import java.util.Map;

import com.ikaver.aagarwal.hw3.common.config.JobInfo;
import com.ikaver.aagarwal.hw3.common.config.SocketAddress;

public class JobsState {
  
  //map Job id --> Host of task manager of job
  //map Job id --> Job state
  
  private Map<Integer, SocketAddress> jobIDToSlave;
  private Map<Integer, JobInfo> jobIDToJobInfo;
  
  public JobInfo getJobInfo(int jobID) {
    return jobIDToJobInfo.get(jobID);
  }
  
  public SocketAddress getRemoteHostOfJob(int jobID) {
    return jobIDToSlave.get(jobID);
  }
  
  public void setJobInfo(JobInfo info) {
    this.jobIDToJobInfo.put(info.getJobID(), info);
  }
  
  public void setRemoteHostOfJob(JobInfo info, SocketAddress host) {
    this.jobIDToSlave.put(info.getJobID(), host);
  }

}
