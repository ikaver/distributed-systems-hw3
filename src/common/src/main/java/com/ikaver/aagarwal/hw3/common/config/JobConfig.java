package com.ikaver.aagarwal.hw3.common.config;

import java.io.Serializable;

public class JobConfig implements Serializable {
  
  private static final long serialVersionUID = 7232089871060033956L;

  private String jobName;
  
  private String inputFilePath;
  private String outputFilePath;
  private int recordSize;
  
  private String jarFilePath;
  private String mapperClass;
  private String reducerClass;
  
  private int numMappers;
  private int numReducers;
  
  private String masterIP;
  private int masterPort;
  
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
  
  public String getJarFilePath() {
    return jarFilePath;
  }
  
  public String getMapperClass() {
    return mapperClass;
  }
  
  public String getReducerClass() {
    return reducerClass;
  }
  
  public int getNumMappers() {
    return numMappers;
  }
  
  public int getNumReducers() {
    return numReducers;
  }
  
  public String getMasterIP() {
    return masterIP;
  }
  
  public int getMasterPort() {
    return masterPort;
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
  
  public void setJarFilePath(String jarFilePath) {
    this.jarFilePath = jarFilePath;
  }
  
  public void setMapperClass(String mapperClass) {
    this.mapperClass = mapperClass;
  }
  
  public void setReducerClass(String reducerClass) {
    this.reducerClass = reducerClass;
  }
  
  public void setNumMappers(int numMappers) {
    this.numMappers = numMappers;
  }
  
  public void setNumReducers(int numReducers) {
    this.numReducers = numReducers;
  }
  
  public void setMasterIP(String masterIP) {
    this.masterIP = masterIP;
  }
  
  public void setMasterPort(int masterPort) {
    this.masterPort = masterPort;
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
