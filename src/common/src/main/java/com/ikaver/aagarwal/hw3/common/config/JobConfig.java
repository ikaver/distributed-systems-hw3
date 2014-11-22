package com.ikaver.aagarwal.hw3.common.config;

import java.io.Serializable;

/**
 * Describes a job configuration given by the client.
 */
public class JobConfig implements Serializable {
  
  private static final long serialVersionUID = 7232089871060033956L;

  private String jobName;
  
  private String inputFilePath;
  private String outputFilePath;
  private int recordSize;
  
  private byte [] jarFile;
  private String mapperClass;
  private String reducerClass;
  
  private int numReducers;
  
  public JobConfig() {
    
  }
  
  public String getJobName() {
    return jobName;
  }
  
  public String getInputFilePath() {
    return inputFilePath;
  }
  
  public String getOutputFilePath() {
    return outputFilePath;
  }
  
  public int getRecordSize() {
    return recordSize;
  }
  
  public byte [] getJarFile() {
    return jarFile;
  }
  
  public String getMapperClass() {
    return mapperClass;
  }
  
  public String getReducerClass() {
    return reducerClass;
  }
  
  public int getNumReducers() {
    return numReducers;
  }
  
  public void setJobName(String jobName) {
    this.jobName = jobName;
  }
  
  public void setInputFilePath(String inputFilePath) {
    this.inputFilePath = inputFilePath;
  }
  
  public void setOutputFilePath(String outputFilePath) {
    this.outputFilePath = outputFilePath;
  }
  
  public void setRecordSize(int recordSize) {
    this.recordSize = recordSize;
  }
  
  public void setJarFile(byte [] jarFile) {
    this.jarFile = jarFile;
  }
  
  public void setMapperClass(String mapperClass) {
    this.mapperClass = mapperClass;
  }
  
  public void setReducerClass(String reducerClass) {
    this.reducerClass = reducerClass;
  }
  
  public void setNumReducers(int numReducers) {
    this.numReducers = numReducers;
  }
  
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append(String.format("[Name : %s ", this.getJobName()));
    builder.append(String.format("Input file: %s ", this.getInputFilePath()));
    builder.append(String.format("Output file: %s ]", this.getOutputFilePath()));
    return builder.toString();
  }
}
