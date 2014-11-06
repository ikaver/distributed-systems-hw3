package com.ikaver.aagarwal.hw3.mrmaster.scheduler;

import java.io.Serializable;

import com.ikaver.aagarwal.hw3.common.util.SocketAddress;
import com.ikaver.aagarwal.hw3.common.workers.WorkerState;

public abstract class WorkerInfo implements Serializable {
  
  private static final long serialVersionUID = -3325713750653604642L;
  
  private final int jobID;
  private final SocketAddress nodeManagerAddress;
  private final WorkerState state;
  
  public WorkerInfo(int jobID, SocketAddress nodeManagerAddr, WorkerState state) {
    if(nodeManagerAddr == null) throw new IllegalArgumentException("Node manager addr cannot be null");
    if(state == null) throw new IllegalArgumentException("Worker state cannot be null");
    this.jobID = jobID;
    this.state = state;
    this.nodeManagerAddress = nodeManagerAddr;
  }

  public int getJobID() {
    return jobID;
  }

  public SocketAddress getNodeManagerAddress() {
    return nodeManagerAddress;
  }

  public WorkerState getState() {
    return state;
  }

}
