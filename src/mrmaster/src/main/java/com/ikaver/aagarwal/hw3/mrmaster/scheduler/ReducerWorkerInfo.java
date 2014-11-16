package com.ikaver.aagarwal.hw3.mrmaster.scheduler;

import com.ikaver.aagarwal.hw3.common.util.SocketAddress;
import com.ikaver.aagarwal.hw3.common.workers.ReduceWorkDescription;
import com.ikaver.aagarwal.hw3.common.workers.WorkerState;

public class ReducerWorkerInfo {
  
  private ReduceWorkDescription workDescription;
  private SocketAddress nodeManagerAddress;
  private WorkerState state;
  
  public ReducerWorkerInfo(ReduceWorkDescription work, SocketAddress socketAddress, WorkerState state) {
    this.workDescription = work;
    this.nodeManagerAddress = socketAddress;
    this.state = state;
  }

  public ReduceWorkDescription getWorkDescription() {
    return workDescription;
  }

  public void setWorkDescription(ReduceWorkDescription work) {
    this.workDescription = work;
  }
  
  public void setNodeManagerAddress(SocketAddress nodeManagerAddress) {
    this.nodeManagerAddress = nodeManagerAddress;
  }

  public SocketAddress getNodeManagerAddress() {
    return nodeManagerAddress;
  }

  public WorkerState getState() {
    return state;
  }

  public void setState(WorkerState state) {
    this.state = state;
  }

}
