package com.ikaver.aagarwal.hw3.mrmaster.scheduler;

import java.util.List;

import com.ikaver.aagarwal.hw3.common.util.SocketAddress;
import com.ikaver.aagarwal.hw3.common.workers.MapperChunk;
import com.ikaver.aagarwal.hw3.common.workers.WorkerState;

public class ReducerWorkerInfo extends WorkerInfo {

  private static final long serialVersionUID = 1377142730548319821L;
  
  private final int reducerID;
  private final List<SocketAddress> inputSources;
  private final List<MapperChunk> mapperChunks;
  private final String outputFilePath;

  public ReducerWorkerInfo(int jobID, SocketAddress nodeManagerAddr, 
      WorkerState state, int reducerID,
      List<SocketAddress> inputSources,
      List<MapperChunk> mapperChunks,
      String outputFilePath) {
    super(jobID, nodeManagerAddr, state);
    this.reducerID = reducerID;
    this.inputSources = inputSources;
    this.mapperChunks = mapperChunks;
    this.outputFilePath = outputFilePath;
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

  public int getReducerID() {
    return reducerID;
  }

}
