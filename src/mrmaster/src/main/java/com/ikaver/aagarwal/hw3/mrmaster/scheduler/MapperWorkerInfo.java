package com.ikaver.aagarwal.hw3.mrmaster.scheduler;

import com.ikaver.aagarwal.hw3.common.util.SocketAddress;
import com.ikaver.aagarwal.hw3.common.workers.MapperChunk;
import com.ikaver.aagarwal.hw3.common.workers.WorkerState;

public class MapperWorkerInfo extends WorkerInfo {
  
  private static final long serialVersionUID = 356190809164040891L;
  
  private final MapperChunk chunk;
  private final String jarFilePath;
  private final String mapperClass;

  public MapperWorkerInfo(int jobID, SocketAddress nodeManagerAddr, 
      WorkerState state,
      MapperChunk chunk,
      String jarFilePath,
      String mapperClassPath) {
    super(jobID, nodeManagerAddr, state);
    if(chunk == null) throw new IllegalArgumentException("Chunk cannot be null");
    if(jarFilePath == null) throw new IllegalArgumentException("Jar file path cannot be null");
    if(mapperClassPath == null) throw new IllegalArgumentException("Mapper class path cannot be null");
    this.chunk = chunk;
    this.jarFilePath = jarFilePath;
    this.mapperClass = mapperClassPath;
  }

  public String getJarFilePath() {
    return jarFilePath;
  }

  public String getMapperClass() {
    return mapperClass;
  }

  public MapperChunk getChunk() {
    return chunk;
  }

}
