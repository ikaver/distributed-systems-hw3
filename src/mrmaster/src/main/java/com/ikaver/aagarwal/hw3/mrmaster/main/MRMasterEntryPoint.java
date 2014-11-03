package com.ikaver.aagarwal.hw3.mrmaster.main;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;

import org.apache.log4j.Logger;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.ikaver.aagarwal.hw3.common.definitions.Definitions;
import com.ikaver.aagarwal.hw3.common.master.IJobManager;

public class MRMasterEntryPoint {
  
  private static final Logger LOG = Logger.getLogger(MRMasterEntryPoint.class);
  
  public static void main(String [] args) {
    MRMasterSettings settings = new MRMasterSettings();
    JCommander argsParser = new JCommander(settings);
    int port = 3000;
    try {
      argsParser.parse(args);
      port = settings.getPort();
    }
    catch (ParameterException ex) {
      argsParser.usage();
    }
    
    
    //Create RMI Registry
    try { 
      LocateRegistry.createRegistry(port); 
      LOG.info("RMI registry created.");
    } catch (RemoteException e) {
      LOG.warn("RMI was already running...");
    }
    
    Injector injector = Guice.createInjector(new MRMasterModule());
    IJobManager jobManager = injector.getInstance(IJobManager.class);
    
    //Start MR Master services
    try {
      Naming.rebind(String.format("//:%d/%s", port, 
          Definitions.JOB_MANAGER_SERVICE), jobManager);
    } catch (RemoteException e) {
      LOG.fatal("Failed to create job manager service", e);
    } catch (MalformedURLException e) {
      LOG.fatal("Failed to create job manager service", e);
    }
  }

}
