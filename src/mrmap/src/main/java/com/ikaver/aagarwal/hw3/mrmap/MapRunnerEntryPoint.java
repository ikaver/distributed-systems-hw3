package com.ikaver.aagarwal.hw3.mrmap;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;

import org.apache.log4j.Logger;

import com.beust.jcommander.JCommander;
import com.ikaver.aagarwal.hw3.common.definitions.Definitions;
import com.ikaver.aagarwal.hw3.common.workers.flags.MRWorkerRunnerSettings;

public class MapRunnerEntryPoint {
	
	private static final Logger LOGGER = Logger.getLogger(MapRunnerEntryPoint.class);

	public static void main(String args[]) throws RemoteException,
			MalformedURLException {
		MRWorkerRunnerSettings settings = new MRWorkerRunnerSettings();
		JCommander cmd = new JCommander(settings);
		cmd.parse(args);

		MapInstanceRunner runner = new MapInstanceRunner();

		LocateRegistry.createRegistry(settings.getPort());

		Naming.rebind(
				String.format("//:%d/" + Definitions.MR_MAP_RUNNER_SERVICE,
						settings.getPort()), runner);

		LOGGER.info("Logger is now running on port" + settings.getPort());
	}
}