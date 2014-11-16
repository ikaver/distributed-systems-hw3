package com.ikaver.aagarwal.hw3.common.workers;

import java.io.Serializable;
import java.util.List;

import com.ikaver.aagarwal.hw3.common.util.SocketAddress;

public class ReduceWorkDescription implements Serializable {

  private static final long serialVersionUID = -8903794081462625776L;

  private final int jobID;
  private final int reducerID;
  private final String reducerClass;
  private final byte [] jarFile;
  private final List<MapWorkDescription> mappers;
  private final List<SocketAddress> mapperAddresses;
  private final String outputFilePath;

  public ReduceWorkDescription(int jobID, int reducerID, 
      String reducerClass,
      List<MapWorkDescription> mappers,
      List<SocketAddress> mapperAddresses,
      String outputFilePath,
      byte [] jarFile) {
    if(reducerClass == null)
      throw new IllegalArgumentException("Reducer class cannot be null");
    if(mappers == null) 
      throw new IllegalArgumentException("Mappers list cannot be null");
    if(mappers.size() == 0)
      throw new IllegalArgumentException("Mappers list cannot be null");
    if(mappers.size() != mapperAddresses.size())
      throw new IllegalArgumentException("Mappers list size should be the same as the mappers addresses list");
    if(outputFilePath == null) 
      throw new IllegalArgumentException("Output file path cannot be null");
    this.jobID = jobID;
    this.reducerID = reducerID;
    this.mappers = mappers;
    this.mapperAddresses = mapperAddresses;
    this.outputFilePath = outputFilePath;
    this.reducerClass = reducerClass;
    this.jarFile = jarFile;
  }

  public int getJobID() {
    return jobID;
  }

  public int getReducerID() {
    return reducerID;
  }
  
  public byte [] getJarFile() {
    return jarFile;
  }

  public String getReducerClass() {
    return reducerClass;
  }

  public List<MapWorkDescription> getMappers() {
    return mappers;
  }

  public List<SocketAddress> getMapperAddresses() {
    return mapperAddresses;
  }

  public String getOutputFilePath() {
    return outputFilePath;
  }
}
