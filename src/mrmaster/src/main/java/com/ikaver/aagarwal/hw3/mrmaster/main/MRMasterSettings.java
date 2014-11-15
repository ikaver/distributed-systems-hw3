package com.ikaver.aagarwal.hw3.mrmaster.main;

import java.util.List;

import com.beust.jcommander.Parameter;

public class MRMasterSettings {
  
  @Parameter(names = "-port", description = "Port at which the mr master services are bound", required = true)
  private int port;
  
  @Parameter(names = "-slaves", description = "List of slave nodes", required = true)
  private List<String> slaves;

  public int getPort() {
    return this.port;
  }
  
  public List<String> getSlaves() {
	  return slaves;
  }
}
