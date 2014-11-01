package com.ikaver.aagarwal.hw3.common.config;

public class Job {
  
  private String jobName;
  
  private String inputFilePath;
  private String outputFilePath;
  private int recordSize;
  
  private byte [] jarFile;
  private String mapperClass;
  private String reducerClass;
  
  private int numMappers;
  private int numReducers;
  
  private String masterIP;
  private int masterPort;
  
  public Job() {
    
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
  
  public byte[] getJarFile() {
    return jarFile;
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
  
  public void setJarFile(byte[] jarFile) {
    this.jarFile = jarFile;
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
}
