package com.ikaver.aagarwal.hw3.mrclient.joblauncher;

import com.beust.jcommander.Parameter;

public class JobLauncherSettings {

  @Parameter(names = "-f", description = "Configuration file path",
      required = true)
  private String configurationFilePath;
  
  public String getConfigurationFilePath() {
    return this.configurationFilePath;
  }
  
}
