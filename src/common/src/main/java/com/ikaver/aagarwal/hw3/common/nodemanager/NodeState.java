package com.ikaver.aagarwal.hw3.common.nodemanager;

import java.io.Serializable;

public class NodeState implements Serializable {

  private static final long serialVersionUID = 2295885179322002521L;

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
