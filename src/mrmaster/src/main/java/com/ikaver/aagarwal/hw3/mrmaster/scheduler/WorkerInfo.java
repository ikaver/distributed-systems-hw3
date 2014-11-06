package com.ikaver.aagarwal.hw3.mrmaster.scheduler;

import java.io.Serializable;

import com.ikaver.aagarwal.hw3.common.util.SocketAddress;

public abstract class WorkerInfo implements Serializable {
  
  private static final long serialVersionUID = -3325713750653604642L;
  
  private final int jobID;
  private final SocketAddress nodeManagerAddress;
  
  public WorkerInfo(int jobID, SocketAddress nodeManagerAddr) {
    if(nodeManagerAddr == null) throw new NullPointerException("Node manager addr cannot be null");
    this.jobID = jobID;
    this.nodeManagerAddress = nodeManagerAddr;
  }

  public int getJobID() {
    return jobID;
  }

  public SocketAddress getNodeManagerAddress() {
    return nodeManagerAddress;
  }

}
