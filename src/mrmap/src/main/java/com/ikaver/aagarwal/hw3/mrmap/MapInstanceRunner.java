package com.ikaver.aagarwal.hw3.mrmap;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

import org.apache.log4j.Logger;

import com.ikaver.aagarwal.hw3.common.mrmap.IMapInstanceRunner;
import com.ikaver.aagarwal.hw3.common.util.SocketAddress;
import com.ikaver.aagarwal.hw3.common.workers.MapWorkDescription;
import com.ikaver.aagarwal.hw3.common.workers.WorkerState;

/**
 * Creates a map runner instance on a separate thread. Responds to node manager
 * for state queries.
 */
public class MapInstanceRunner extends UnicastRemoteObject implements
		IMapInstanceRunner {

	private static final long serialVersionUID = -2794021388396287951L;
	private static final Logger LOGGER = Logger
			.getLogger(MapInstanceRunner.class);

	private MapRunner runner;
	private Thread mapRunnerThread;
	private final SocketAddress masterAddress;
	
	protected MapInstanceRunner(SocketAddress masterAddress) throws RemoteException {
		super();
		this.masterAddress = masterAddress;
	}

	/**
	 * Somehow need to know the name of the input file on which this mapper
	 * needs to act.
	 */
	public void runMapInstance(MapWorkDescription input) {
	  LOGGER.info("MapInstanceRunner got job: " + input.getJobID() + " " + 
	         input.getChunk().getInputFilePath());
		this.runner = new MapRunner(input, masterAddress);
		this.mapRunnerThread = new Thread(runner);
		this.mapRunnerThread.start();
	}

	/**
	 * Die in peace JVM. You have done your duty.
	 */
	public void die() {
		System.exit(1);
	}

	public WorkerState getMapperState() {
	  return this.runner.getMapWorkState().getState();
	}

	public String getMapOutputFilePath() {
	  return this.runner.getMapWorkState().getOutputPath();
	}
	

}
