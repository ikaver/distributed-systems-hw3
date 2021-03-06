package com.ikaver.aagarwal.hw3.common.config;

import java.io.Serializable;

/***
 * Simple class that describes the progress of a job in the MR system.
 */
public class JobInfoForClient implements Serializable {

  private static final long serialVersionUID = 5024005953254650962L;
  private final int jobID;
  private final String jobName;
  private final int numMappers;
  private final int numReducers;

  private final int numMappersCompleted;
  private final int numReducersCompleted;

  public JobInfoForClient(int jobID, String jobName, int numMappers, int numReducers,
      int numMappersCompleted, int numReducersCompleted) {
    this.jobID = jobID;
    this.jobName = jobName;
    this.numMappers = numMappers;
    this.numReducers = numReducers;
    this.numMappersCompleted = numMappersCompleted;
    this.numReducersCompleted = numReducersCompleted;
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

  @Override
  public String toString() {
    return String.format(
        "[(Job ID: %d), (Job name: %s), (Mappers completed: %d), (Reducers completed: %d)]",
        this.jobID, this.jobName, this.numMappersCompleted, this.numReducersCompleted
    );
  }

}
