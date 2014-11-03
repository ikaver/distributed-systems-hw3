package com.ikaver.aagarwal.hw3.mrclient.jobmonitor;

import com.beust.jcommander.Parameter;

public class JobMonitorSettings {
  
  @Parameter(names = "-port", description = "MR Master port", required = true)
  private int masterPort;
  
  @Parameter(names = "-host", description = "MR Master host", required = true)
  private String masterHost;

  public int getMasterPort() {
    return this.masterPort;
  }
  
  public String getMasterHost() {
    return this.masterHost;
  }
  

}
