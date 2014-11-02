package com.ikaver.aagarwal.hw3.mrslave.runner;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

import org.apache.log4j.Logger;

import com.ikaver.aagarwal.hw3.common.config.MRMapTaskInput;
import com.ikaver.aagarwal.hw3.common.definitions.Definitions;
import com.ikaver.aagarwal.hw3.common.mrmap.IMapInstanceRunner;
import com.ikaver.aagarwal.hw3.common.slave.IMRTaskManager;

/**
 * A task manager manages a "slave" node. Following are the responsibilities of
 * a task manager.
 * 1. Accepts a jar and forks out separate jvms for running mapper and reducer
 *   tasks.
 * 2. Periodically updates master with the status of the map reduce
 *   job assigned to it.
 */
public class MRTaskManager extends UnicastRemoteObject implements IMRTaskManager  {

	protected MRTaskManager() throws RemoteException {
		super();
	}

	private static final long serialVersionUID = 1674990898801584371L;
	private static final Logger LOGGER = Logger.getLogger(MRTaskManager.class);

	/**
	 * Following is the sequence of operations which should be executed by the doMap function.
	 * 1. Fetch data from DFS and copy to the local disk.
	 * 2. Copy the jars from the DFS to the local disk.
	 * 3. Pass the local path from (1) and (2) to the mapper task.
	 */
	// TODO(ankit): Return a failure error code.
	@SuppressWarnings("resource")
	public void doMap(MRMapTaskInput input) {
		int port = MRMapTaskAttempt.startMapTask(input);
		LOGGER.info(String.format("Starting map runner at port: %d", port));
		try {
			IMapInstanceRunner runner =
					(IMapInstanceRunner) Naming.lookup(String.format("//%s:%d/%s",
					"localhost",
					port,
					Definitions.MR_MAP_RUNNER));

			runner.runMapInstance(input);
			
			// killing runner now!
			runner.die();

		} catch (MalformedURLException e) {
		} catch (RemoteException e) {
		} catch (NotBoundException e) {
		}
		
	}

	public void doReduce() throws RemoteException {
		// TODO Auto-generated method stub
		
	}
}
