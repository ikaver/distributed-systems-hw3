package com.ikaver.aagarwal.hw3.jobcreator;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;

public class JobCreator {
  
  
  
  public static void main(String [] args) {
    JobCreatorSettings settings = new JobCreatorSettings();
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
