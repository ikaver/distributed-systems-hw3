package com.ikaver.aagarwal.hw3.common.config;

/***
 * Simple class that describes the progress of a job in the MR system.
 */
public class JobInfo {
  
  private final int jobID;
  private final String jobName;
  private final int numMappers;
  private final int numReducers;
  
  private final int numMappersCompleted;
  private final int numReducersCompleted;
  
  public JobInfo(int jobID, String jobName, int numMappers, int numReducers,
      int numMappersCompleted, int numReducersCompleted) {
    this.jobID = jobID;
    this.jobName = jobName;
    this.numMappers = numMappers;
    this.numReducers = numReducers;
    this.numMappersCompleted = numMappersCompleted;
    this.numReducersCompleted = numReducersCompleted;
  }
  
  public JobInfo(int jobID, Job job) {
    this.jobID = jobID;
    this.jobName = job.getJobName();
    this.numMappers = job.getNumMappers();
    this.numReducers = job.getNumReducers();
    this.numMappersCompleted = 0;
    this.numReducersCompleted = 0;
  }
  
  public int getJobID() {
    return jobID;
  }

  public String getJobName() {
    return jobName;
  }

  public int getNumMappers() {
    return numMappers;
  }

  public int getNumReducers() {
    return numReducers;
  }

  public int getNumMappersCompleted() {
    return numMappersCompleted;
  }

  public int getNumReducersCompleted() {
    return numReducersCompleted;
  }  
 
}
