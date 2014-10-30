package com.ikaver.aagarwal.hw3.mrclient.joblauncher;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.ikaver.aagarwal.hw3.common.config.Job;

public class JobLauncher {
  
  public void launchJob(Job job) {

  }
  
  public static void main(String [] args) {
    JobLauncherSettings settings = new JobLauncherSettings();
    JCommander argsParser = new JCommander(settings);
    try {
      argsParser.parse(args);
    }
    catch (ParameterException ex) {
      argsParser.usage();
      System.exit(-1);
    }
    
    String filePath = settings.getConfigurationFilePath();
    
  }
  
}
