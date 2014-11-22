package com.ikaver.aagarwal.hw3.mrmaster.scheduler;

import com.ikaver.aagarwal.hw3.common.util.SocketAddress;
import com.ikaver.aagarwal.hw3.common.workers.MapWorkDescription;
import com.ikaver.aagarwal.hw3.common.workers.WorkerState;

/**
 * Describes a worker that is responsible of a mapper task.
 */
public class MapperWorkerInfo {

  private MapWorkDescription workDescription;
  private SocketAddress nodeManagerAddress;
  private WorkerState state;
  
  public MapperWorkerInfo(MapWorkDescription work, SocketAddress socketAddress, WorkerState state) {
    this.workDescription = work;
    this.nodeManagerAddress = socketAddress;
    this.state = state;
  }

  public MapWorkDescription getWorkDescription() {
    return workDescription;
  }

  public void setWorkDescription(MapWorkDescription work) {
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
