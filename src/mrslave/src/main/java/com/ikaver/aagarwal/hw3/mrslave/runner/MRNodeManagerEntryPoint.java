package com.ikaver.aagarwal.hw3.mrslave.runner;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;

import org.apache.log4j.Logger;

import com.beust.jcommander.JCommander;
import com.ikaver.aagarwal.hw3.common.definitions.Definitions;
import com.ikaver.aagarwal.hw3.common.workers.MapWorkDescription;

/**
 * Entry point for task runner.
 *
 */
public class MRNodeManagerEntryPoint {
	
	private static final Logger LOGGER = Logger.getLogger(MRNodeManagerEntryPoint.class);

	public static void main(String args[]) throws RemoteException, MalformedURLException {

		MRNodeManager manager = new MRNodeManager();
		
		MRNodeManagerSettings settings = new MRNodeManagerSettings();

		JCommander cmd = new JCommander(settings);
		cmd.parse(args);

		LocateRegistry.createRegistry(settings.getPort());
		Naming.rebind(String.format("//:%d/" + Definitions.MR_TASK_MANAGER, settings.getPort()), manager);
		
		MapWorkDescription input = new  MapWorkDescription(
				0, null, "/home/ankit/git/distributed-systems-hw3/src/common/target/common-1.0-SNAPSHOT-jar-with-dependencies.jar",
				"com.ikaver.aagarwal.hw3.common.examples.WordCountMapper");

		manager.doMap(input);

		LOGGER.info(String.format("MR Task manager is now running at port %d", settings.getPort()));
	}
}
