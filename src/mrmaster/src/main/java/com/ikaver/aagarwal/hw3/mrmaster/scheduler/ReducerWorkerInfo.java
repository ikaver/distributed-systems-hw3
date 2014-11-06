package com.ikaver.aagarwal.hw3.mrmaster.scheduler;

import com.ikaver.aagarwal.hw3.common.util.SocketAddress;

public class ReducerWorkerInfo extends WorkerInfo {

  private static final long serialVersionUID = 1377142730548319821L;
  
  private final int reducerID;

  public ReducerWorkerInfo(int jobID, SocketAddress nodeManagerAddr, int reducerID) {
    super(jobID, nodeManagerAddr);
    this.reducerID = reducerID;
  }

  public int getReducerID() {
    return reducerID;
  }

}
