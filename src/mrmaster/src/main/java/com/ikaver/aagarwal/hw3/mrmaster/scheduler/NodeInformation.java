package com.ikaver.aagarwal.hw3.mrmaster.scheduler;

import com.ikaver.aagarwal.hw3.common.util.SocketAddress;

/**
 * Contains the current state of a node manager.
 */
public class NodeInformation {

  private SocketAddress nodeAddress;
  private int availableSlots;
  private int numCores;
  
  public NodeInformation(SocketAddress addr, int numCores, int availableSlots) {
    this.nodeAddress = addr;
    this.availableSlots = availableSlots;
    this.availableSlots = availableSlots;
  }
  
  public SocketAddress getNodeAddress() {
    return nodeAddress;
  }

  public int getNumCores() {
    return numCores;
  }

  public int getAvailableSlots() {
    return availableSlots;
  } 
  
  public void setAvailableSlots(int slots) {
    if(slots > 0) {
      this.availableSlots = slots;
    }
    else {
      this.availableSlots = 0;
    }
  }

}
