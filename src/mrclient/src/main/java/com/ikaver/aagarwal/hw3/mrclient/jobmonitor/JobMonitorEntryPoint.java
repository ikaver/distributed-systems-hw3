package com.ikaver.aagarwal.hw3.mrclient.jobmonitor;

import org.apache.log4j.Logger;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.ikaver.aagarwal.hw3.common.config.MRConfig;
import com.ikaver.aagarwal.hw3.common.util.SocketAddress;

/**
 * Entry point for the client job monitor project.
 */
public class JobMonitorEntryPoint {
  
  private static final Logger LOG = Logger.getLogger(JobMonitorEntryPoint.class);

  public static void main(String [] args) {
    //Get command line args
    JobMonitorSettings settings = new JobMonitorSettings();
    JCommander argsParser = new JCommander(settings);
    String configFilePath = null;
    try {
      argsParser.parse(args);
      configFilePath = settings.getConfigFilePath();
    } catch (ParameterException ex) {
      argsParser.usage();
      System.exit(-1);
    }

    //Setup configuration
    if(!MRConfig.setupFromConfigFile(configFilePath)) {
      LOG.error("Failed to read setup file.");
      System.exit(-1);
    }
        
    //Create job monitor
    SocketAddress masterAddr = MRConfig.getMasterSocketAddress();
    Injector injector = Guice.createInjector(new JobMonitorModule(masterAddr));
    JobMonitorController controller = injector.getInstance(JobMonitorController.class);
    controller.start();
  }

}
