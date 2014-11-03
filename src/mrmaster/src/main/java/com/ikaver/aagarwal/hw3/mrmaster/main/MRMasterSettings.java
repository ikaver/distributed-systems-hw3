package com.ikaver.aagarwal.hw3.mrmaster.main;

import com.beust.jcommander.Parameter;

public class MRMasterSettings {
  
  @Parameter(names = "-port", description = "Port at which the mr m", required = true)
  private int port;

  public int getPort() {
    return this.port;
  }
  
  
}
