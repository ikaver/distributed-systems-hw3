package com.ikaver.aagarwal.hw3.mrnodemanager.runner;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;

import org.apache.log4j.Logger;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.ikaver.aagarwal.hw3.common.config.MRConfig;
import com.ikaver.aagarwal.hw3.common.definitions.Definitions;
import com.ikaver.aagarwal.hw3.common.dfs.IDataNode;
import com.ikaver.aagarwal.hw3.common.nodemanager.IMRNodeManager;
import com.ikaver.aagarwal.hw3.common.util.SocketAddress;

/**
 * Entry point for task runner.
 * 
 */
public class MRNodeManagerEntryPoint {

  private static final Logger LOG = Logger
      .getLogger(MRNodeManagerEntryPoint.class);

  public static void main(String args[]) throws RemoteException,
  MalformedURLException {

    MRNodeManagerSettings settings = new MRNodeManagerSettings();
    JCommander cmd = new JCommander(settings);
    String configFilePath = null;
    try {
      cmd.parse(args);
      configFilePath = settings.getConfigFilePath();
    } catch (ParameterException ex) {
      cmd.usage();
      System.exit(-1);
    }

    if(!MRConfig.setupFromConfigFile(configFilePath)) {
      LOG.error("Failed to read setup file.");
      System.exit(-1);
    }

    SocketAddress masterAddr = MRConfig.getMasterSocketAddress();
    Injector injector = Guice.createInjector(new MRNodeManagerModule(masterAddr));
    IMRNodeManager manager = injector.getInstance(IMRNodeManager.class);
    IDataNode dataNode = injector.getInstance(IDataNode.class);

    LocateRegistry.createRegistry(settings.getPort());
    Naming.rebind(String.format("//:%d/"
        + Definitions.MR_NODE_MANAGER_SERVICE, settings.getPort()), manager);
    Naming.rebind(
        String.format("//:%d/" + Definitions.DATA_NODE_SERVICE, settings.getPort()),
        dataNode);

    LOG.info(String.format("MR Task manager is now running at port %d",
        settings.getPort()));
  }
}
