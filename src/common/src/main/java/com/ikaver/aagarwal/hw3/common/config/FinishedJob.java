package com.ikaver.aagarwal.hw3.common.config;

import java.io.Serializable;

public class FinishedJob implements Serializable {

  private static final long serialVersionUID = 8329718442147537881L;
  private int jobID;
  private String jobName;
  private boolean finishedSuccessfully;
  private String outputPath;
  private int numReducers;
  
  public FinishedJob(int jobID, String jobName, boolean finishedSuccessfully, 
      String outputPath, int numReducers) {
    this.jobID = jobID;
    this.jobName = jobName;
    this.finishedSuccessfully = finishedSuccessfully;
    this.outputPath = outputPath;
    this.numReducers = numReducers;
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
  
  public String getOutputPath() {
    return outputPath;
  }

  public int getNumReducers() {
    return numReducers;
  }

  @Override
  public String toString() {
    StringBuilder outputFiles = new StringBuilder("{ ");
    for(int i = 0; i < this.getNumReducers(); ++i) {
      outputFiles.append(getOutputPath() + "-" + i + ".out ");
    }
    outputFiles.append("}");
    return String.format("[ID: %d, Name: %s, Success: %b, Output: %s]", getJobID(), 
        getJobName(), isFinishedSuccessfully(), outputFiles.toString());
  }

}
