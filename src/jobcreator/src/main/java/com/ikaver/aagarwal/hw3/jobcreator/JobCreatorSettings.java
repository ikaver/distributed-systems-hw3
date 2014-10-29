package com.ikaver.aagarwal.hw3.jobcreator;

import com.beust.jcommander.Parameter;

public class JobCreatorSettings {

  @Parameter(names = "-f", description = "Configuration file path",
      required = true)
  private String configurationFilePath;
  
  public String getConfigurationFilePath() {
    return this.configurationFilePath;
  }
  
}
