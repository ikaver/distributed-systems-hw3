package com.ikaver.aagarwal.hw3.common.config;

import java.io.Serializable;

public class FinishedJob implements Serializable {

  private static final long serialVersionUID = 8329718442147537881L;
  private int jobID;
  private String jobName;
  private boolean finishedSuccessfully;
  
  public FinishedJob(int jobID, String jobName, boolean finishedSuccessfully) {
    this.jobID = jobID;
    this.jobName = jobName;
    this.finishedSuccessfully = finishedSuccessfully;
  }

  public int getJobID() {
    return jobID;
  }

  public String getJobName() {
    return jobName;
  }

  public boolean isFinishedSuccessfully() {
    return finishedSuccessfully;
  }
  
  @Override
  public String toString() {
    return String.format("[ID: %d, Name: %s, Success: %b", getJobID(), 
        getJobName(), isFinishedSuccessfully());
  }
  

}
