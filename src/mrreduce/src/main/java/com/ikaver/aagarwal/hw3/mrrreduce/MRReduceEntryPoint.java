package com.ikaver.aagarwal.hw3.mrrreduce;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;

import org.apache.log4j.Logger;

import com.beust.jcommander.JCommander;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.ikaver.aagarwal.hw3.common.definitions.Definitions;
import com.ikaver.aagarwal.hw3.common.mrreduce.IMRReduceInstanceRunner;
import com.ikaver.aagarwal.hw3.common.util.SocketAddress;

/**
 * Entry point for an instance which runs a "reduce" instance.
 */
public class MRReduceEntryPoint {

	private static final Logger LOGGER = Logger
			.getLogger(MRReduceEntryPoint.class);

	public static void main(String args[]) throws RemoteException,
			MalformedURLException {

		MRReduceSettings settings = new MRReduceSettings();

		JCommander cmd = new JCommander(settings);
		cmd.parse(args);
		
		SocketAddress masterAddress = new SocketAddress(settings.getMasterIP(),
				settings.getMasterPort());

		Injector injector = Guice.createInjector(new MRReduceModule(masterAddress));

		IMRReduceInstanceRunner runner =
				injector.getInstance(IMRReduceInstanceRunner.class);

		LocateRegistry.createRegistry(settings.getPort());

		Naming.rebind(String.format("//:%d/"
				+ Definitions.MR_REDUCE_RUNNER_SERVICE, settings.getPort()),
				runner);

		LOGGER.info("Logger is now running on port" + settings.getPort());

	}

}
