package com.ikaver.aagarwal.hw3.mrmaster.main;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.ikaver.aagarwal.hw3.common.definitions.Definitions;
import com.ikaver.aagarwal.hw3.common.dfs.IDFS;
import com.ikaver.aagarwal.hw3.common.master.IJobManager;
import com.ikaver.aagarwal.hw3.common.util.SocketAddress;

public class MRMasterEntryPoint {

	private static final Logger LOG = Logger
			.getLogger(MRMasterEntryPoint.class);

	public static void main(String[] args) {
		MRMasterSettings settings = new MRMasterSettings();
		JCommander argsParser = new JCommander(settings);
		int port = -1;
		try {
			argsParser.parse(args);
			port = settings.getPort();
		} catch (ParameterException ex) {
			argsParser.usage();
			System.exit(-1);
		}

		// Create RMI Registry
		try {
			LocateRegistry.createRegistry(port);
			LOG.info("RMI registry created.");
		} catch (RemoteException e) {
			LOG.warn("RMI was already running...");
		}

		Set<SocketAddress> nodes = getNodes(settings.getSlaves());

		Injector injector = Guice.createInjector(new MRMasterModule(nodes));
		IJobManager jobManager = injector.getInstance(IJobManager.class);
		IDFS dfs = injector.getInstance(IDFS.class);
		// Start MR Master services
		try {
			Naming.rebind(String.format("//:%d/%s", port,
					Definitions.JOB_MANAGER_SERVICE), jobManager);
			Naming.rebind(
					String.format("//:%d/%s", port, Definitions.DFS_SERVICE),
					dfs);
		} catch (RemoteException e) {
			LOG.fatal("Failed to create master services", e);
		} catch (MalformedURLException e) {
			LOG.fatal("Failed to create master services", e);
		}
	}

	private static Set<SocketAddress> getNodes(String slaves) {
		StringTokenizer tokenizer = new StringTokenizer(slaves, ",");
		
		Set<SocketAddress> nodes = new HashSet<SocketAddress>();
		while (tokenizer.hasMoreTokens()) {
			SocketAddress address = new SocketAddress(tokenizer.nextToken(),
					Definitions.NODE_MANAGER_SERVICE_PORT);
			LOG.info("Adding node with ip address:" + address.getHostname());
			nodes.add(address);
		}
		return nodes;
	}

}
