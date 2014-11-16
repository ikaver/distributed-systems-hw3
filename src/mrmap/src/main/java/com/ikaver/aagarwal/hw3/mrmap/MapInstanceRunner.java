package com.ikaver.aagarwal.hw3.mrmap;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

import org.apache.log4j.Logger;

import com.ikaver.aagarwal.hw3.common.mrmap.IMapInstanceRunner;
import com.ikaver.aagarwal.hw3.common.workers.MapWorkDescription;
import com.ikaver.aagarwal.hw3.common.workers.WorkerState;

public class MapInstanceRunner extends UnicastRemoteObject implements
		IMapInstanceRunner {

	private static final long serialVersionUID = -2794021388396287951L;
	private static final Logger LOGGER = Logger
			.getLogger(MapInstanceRunner.class);

	private MapRunner runner;
	private Thread mapRunnerThread;
	
	protected MapInstanceRunner() throws RemoteException {
		super();
	}

	/**
	 * Somehow need to know the name of the input file on which this mapper
	 * needs to act.
	 */
	public void runMapInstance(MapWorkDescription input, String localFilePath) {
		LOGGER.info("Received " + input.getJobID() + " " + input.getChunk().getInputFilePath() + " " + localFilePath);
		this.runner = new MapRunner(input, localFilePath);
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
