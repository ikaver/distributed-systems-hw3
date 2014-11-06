package com.ikaver.aagarwal.hw3.common.nodemanager;

public class NodeState {
  
  private final int numMappers;
  private final int numReducers;
  
  private final int availableMappers;
  private final int availableReducers;
  
  public NodeState(int numMappers, int numReducers, int availableMappers,
      int availableReducers) {
    this.numMappers = numMappers;
    this.numReducers = numReducers;
    this.availableMappers = availableMappers;
    this.availableReducers = availableReducers;
  }

  public int getNumMappers() {
    return numMappers;
  }

  public int getNumReducers() {
    return numReducers;
  }

  public int getAvailableMappers() {
    return availableMappers;
  }

  public int getAvailableReducers() {
    return availableReducers;
  }
  
  

}
