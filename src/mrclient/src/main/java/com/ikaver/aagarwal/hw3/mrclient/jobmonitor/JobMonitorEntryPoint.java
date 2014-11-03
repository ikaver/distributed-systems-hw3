package com.ikaver.aagarwal.hw3.mrclient.jobmonitor;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.google.inject.Guice;
import com.google.inject.Injector;

public class JobMonitorEntryPoint {

  public static void main(String [] args) {
    JobMonitorSettings settings = new JobMonitorSettings();
    JCommander argsParser = new JCommander(settings);
    try {
      argsParser.parse(args);
    }
    catch (ParameterException ex) {
      argsParser.usage();
    }
    
    Injector injector = Guice.createInjector(
        new JobMonitorModule(settings.getMasterHost(), settings.getMasterPort()));
    JobMonitorController controller = injector.getInstance(JobMonitorController.class);
    controller.start();
  }

}
