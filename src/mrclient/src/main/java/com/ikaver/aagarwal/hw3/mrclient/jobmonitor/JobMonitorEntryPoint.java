package com.ikaver.aagarwal.hw3.mrclient.jobmonitor;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.google.inject.Guice;
import com.google.inject.Injector;

public class JobMonitorEntryPoint {

  public static void main(String [] args) {
    JobMonitorSettings settings = new JobMonitorSettings();
    JCommander argsParser = new JCommander(settings);
    String host = null;
    int port = -1;
    try {
      argsParser.parse(args);
      host = settings.getMasterHost();
      port = settings.getMasterPort();
    }
    catch (ParameterException ex) {
      argsParser.usage();
      System.exit(-1);
      //TODO: quit program here
    }
    
    Injector injector = Guice.createInjector(
        new JobMonitorModule(host, port));
    JobMonitorController controller = injector.getInstance(JobMonitorController.class);
    controller.start();
  }

}
