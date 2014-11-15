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
import com.ikaver.aagarwal.hw3.common.definitions.Definitions;
import com.ikaver.aagarwal.hw3.common.dfs.IDataNode;
import com.ikaver.aagarwal.hw3.common.nodemanager.IMRNodeManager;
import com.ikaver.aagarwal.hw3.common.workers.MapWorkDescription;

/**
 * Entry point for task runner.
 *
 */
public class MRNodeManagerEntryPoint {
	
	private static final Logger LOGGER = Logger.getLogger(MRNodeManagerEntryPoint.class);

	public static void main(String args[]) throws RemoteException, MalformedURLException {
		
		MRNodeManagerSettings settings = new MRNodeManagerSettings();
    int port = -1;

		JCommander cmd = new JCommander(settings);
    try {
      cmd.parse(args);
      port = settings.getPort();
    }
    catch (ParameterException ex) {
      cmd.usage();
      System.exit(-1);
    }
    
    
    Injector injector = Guice.createInjector(new MRNodeManagerModule());
    IMRNodeManager manager = injector.getInstance(IMRNodeManager.class);
    IDataNode dataNode = injector.getInstance(IDataNode.class);
    
		LocateRegistry.createRegistry(settings.getPort());
		Naming.rebind(String.format("//:%d/" + Definitions.MR_NODE_MANAGER_SERVICE, port), manager);
    Naming.rebind(String.format("//:%d/" + Definitions.DATA_NODE_SERVICE, port), dataNode);
		
		//MapWorkDescription input = new  MapWorkDescription(
		//		0, null, "/home/ankit/git/distributed-systems-hw3/src/common/target/common-1.0-SNAPSHOT-jar-with-dependencies.jar",
		//		"com.ikaver.aagarwal.hw3.common.examples.WordCountMapper");

		//manager.doMap(input);

		LOGGER.info(String.format("MR Task manager is now running at port %d", settings.getPort()));
	}
}
