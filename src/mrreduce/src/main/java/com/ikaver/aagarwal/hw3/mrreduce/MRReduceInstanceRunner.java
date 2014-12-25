package com.ikaver.aagarwal.hw3.mrreduce;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

import org.apache.log4j.Logger;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.ikaver.aagarwal.hw3.common.definitions.Definitions;
import com.ikaver.aagarwal.hw3.common.mrreduce.IMRReduceInstanceRunner;
import com.ikaver.aagarwal.hw3.common.util.SocketAddress;
import com.ikaver.aagarwal.hw3.common.workers.ReduceWorkDescription;
import com.ikaver.aagarwal.hw3.common.workers.WorkerState;

/**
 * Creates a reduce runner instance on a separate thread. Responds to node manager
 * for state queries.
 */
@Singleton
public class MRReduceInstanceRunner extends UnicastRemoteObject implements
		IMRReduceInstanceRunner {

	private static final long serialVersionUID = -5818018149085953673L;
	private static final Logger LOGGER = Logger
			.getLogger(MRReduceInstanceRunner.class);

	private final SocketAddress masterAddress;
	private MRReduceRunner runner;

	@Inject
	public MRReduceInstanceRunner(
			@Named(Definitions.MASTER_SOCKET_ADDR_ANNOTATION) SocketAddress masterAddress)
			throws RemoteException {
		this.masterAddress = masterAddress;
	}

	public void runReduceInstance(ReduceWorkDescription rwd)
			throws RemoteException {
		this.runner = new MRReduceRunner(rwd, masterAddress);
		LOGGER.info("Forking a separate thread for running map reduce" +
				"runner.");
		new Thread(runner).start();	
	}

	public WorkerState getReducerState() throws RemoteException {
		return runner.getReducerState();
	}

	public void die() throws RemoteException {
		System.exit(1);
	}
}
