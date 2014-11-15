package com.ikaver.aagarwal.hw3.mrmaster.main;

import com.beust.jcommander.Parameter;

public class MRMasterSettings {
  
  @Parameter(names = "-port", description = "Port at which the mr master services are bound", required = true)
  private int port;
  
  @Parameter(names = "-slaves", description = "List of comma seperated slave nodes", required = true)
  private String slaves;

  public int getPort() {
    return this.port;
  }
  
  public String getSlaves() {
	  return slaves;
  }
}
