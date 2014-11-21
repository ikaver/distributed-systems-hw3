package com.ikaver.aagarwal.hw3.mrmaster.main;

import com.beust.jcommander.Parameter;

public class MRMasterSettings {
  
  @Parameter(names = "-config", description = "System config file path", required = true)
  private String configFilePath;
  
  public String getConfigFilePath() {
    return configFilePath;
  }
  
}
