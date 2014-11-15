package com.ikaver.aagarwal.hw3.common.workers;

import java.io.Serializable;
import java.util.List;

import com.ikaver.aagarwal.hw3.common.util.SocketAddress;

public class ReduceWorkDescription implements Serializable {

  private static final long serialVersionUID = -8903794081462625776L;

  private final int jobID;
  private final int reducerID;
  private final byte [] jarFile;
  private final List<SocketAddress> inputSources;
  private final List<MapperChunk> mapperChunks;
  private final String outputFilePath;

  public ReduceWorkDescription(int jobID, int reducerID, 
      List<SocketAddress> inputSources, 
      List<MapperChunk> chunks /* Get the partition id of each input source */,
      String outputFilePath,
      byte [] jarFile) {
    if(inputSources == null) 
      throw new IllegalArgumentException("Input sources cannot be null");
    if(chunks == null) 
      throw new IllegalArgumentException("Chunks cannot be null");
    if(outputFilePath == null) 
      throw new IllegalArgumentException("Output file path cannot be null");
    if(inputSources.size() != chunks.size()) 
      throw new IllegalArgumentException("Amount of input sources must be equal to amount of chunks");
    this.jobID = jobID;
    this.reducerID = reducerID;
    this.inputSources = inputSources;
    this.mapperChunks = chunks;
    this.outputFilePath = outputFilePath;
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

  public List<SocketAddress> getInputSources() {
    return inputSources;
  }

  public List<MapperChunk> getMapperChunks() {
    return mapperChunks;
  }

  public String getOutputFilePath() {
    return outputFilePath;
  }
}
