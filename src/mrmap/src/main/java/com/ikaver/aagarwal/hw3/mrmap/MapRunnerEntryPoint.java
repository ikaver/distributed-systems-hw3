package com.ikaver.aagarwal.hw3.mrmap;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;

import com.beust.jcommander.JCommander;
import com.ikaver.aagarwal.hw3.common.definitions.Definitions;

public class MapRunnerEntryPoint {

	public static void main(String args[]) throws RemoteException,
			MalformedURLException {
		MapRunnerSettings settings = new MapRunnerSettings();
		JCommander cmd = new JCommander(settings);
		cmd.parse(args);

		MapInstanceRunner runner = new MapInstanceRunner();

		LocateRegistry.createRegistry(settings.getPort());

		Naming.rebind(
				String.format("//:%d/" + Definitions.MR_MAP_RUNNER,
						settings.getPort()), runner);
	}
}