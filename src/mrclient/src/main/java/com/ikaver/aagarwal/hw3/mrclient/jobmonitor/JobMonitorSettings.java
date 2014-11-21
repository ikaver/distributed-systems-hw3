package com.ikaver.aagarwal.hw3.mrclient.jobmonitor;

import com.beust.jcommander.Parameter;

public class JobMonitorSettings {
  
  @Parameter(names = "-config", description = "System config file path", required = true)
  private String configFilePath;
  
  public String getConfigFilePath() {
    return configFilePath;
  }

}
