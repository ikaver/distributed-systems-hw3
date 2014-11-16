package com.ikaver.aagarwal.hw3.common.nodemanager;

import java.io.Serializable;

public class NodeState implements Serializable {

  private static final long serialVersionUID = 2295885179322002521L;

  private final int numMappers;
  private final int numReducers;
  
  private final int numProcessors;
  private final int availableSlots;
  
  public NodeState(int numMappers, int numReducers,
      int availableSlots, int numProcessors) {
    this.numMappers = numMappers;
    this.numReducers = numReducers;
    this.availableSlots = availableSlots;
    this.numProcessors = numProcessors;
  }

  public int getNumProcessors() {
    return numProcessors;
  }

  public int getNumMappers() {
    return numMappers;
  }

  public int getNumReducers() {
    return numReducers;
  }

  public int getAvailableSlots() {
    return availableSlots;
  }
  
  

}
