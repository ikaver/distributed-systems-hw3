package com.ikaver.aagarwal.hw3.mrmaster.scheduler;

import com.ikaver.aagarwal.hw3.common.util.SocketAddress;
import com.ikaver.aagarwal.hw3.common.workers.MapperChunk;
import com.ikaver.aagarwal.hw3.common.workers.WorkerState;

public class MapperWorkerInfo extends WorkerInfo {
  
  private static final long serialVersionUID = 356190809164040891L;
  
  private final MapperChunk chunk;

  public MapperWorkerInfo(int jobID, SocketAddress nodeManagerAddr, 
      WorkerState state,
      MapperChunk chunk) {
    super(jobID, nodeManagerAddr, state);
    if(chunk == null) throw new IllegalArgumentException("Chunk cannot be null");
    this.chunk = chunk;
  }

  public MapperChunk getChunk() {
    return chunk;
  }


}
