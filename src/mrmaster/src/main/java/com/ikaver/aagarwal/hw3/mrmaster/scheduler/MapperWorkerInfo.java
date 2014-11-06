package com.ikaver.aagarwal.hw3.mrmaster.scheduler;

import com.ikaver.aagarwal.hw3.common.util.SocketAddress;
import com.ikaver.aagarwal.hw3.common.workers.MapperChunk;

public class MapperWorkerInfo extends WorkerInfo {
  
  private static final long serialVersionUID = 356190809164040891L;
  
  private final MapperChunk chunk;

  public MapperWorkerInfo(int jobID, SocketAddress nodeManagerAddr, 
      MapperChunk chunk) {
    super(jobID, nodeManagerAddr);
    if(chunk == null) throw new NullPointerException("Chunk cannot be null");
    this.chunk = chunk;
  }

  public MapperChunk getChunk() {
    return chunk;
  }


}
