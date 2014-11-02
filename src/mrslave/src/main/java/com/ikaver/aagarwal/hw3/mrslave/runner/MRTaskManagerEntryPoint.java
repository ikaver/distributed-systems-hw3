package com.ikaver.aagarwal.hw3.mrslave.runner;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.RemoteException;

import org.apache.log4j.Logger;

import com.beust.jcommander.JCommander;
import com.ikaver.aagarwal.hw3.common.definitions.Definitions;

/**
 * Entry point for task runner.
 *
 */
public class MRTaskManagerEntryPoint {
	
	private static final Logger LOGGER = Logger.getLogger(MRTaskManagerEntryPoint.class);

	public static void main(String args[]) throws RemoteException, MalformedURLException {

		MRTaskManager manager = new MRTaskManager();
		
		MRTaskManagerSettings settings = new MRTaskManagerSettings();

		JCommander cmd = new JCommander(settings);
		cmd.parse(args);

		Naming.rebind(String.format("//:%d/" + Definitions.MR_TASK_MANAGER, settings.getPort()), manager);
		LOGGER.info("MR Task manager is now running.");
	}
}
