package com.ikaver.aagarwal.hw3.mrnodemanager.runner;

import java.io.Serializable;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.List;

import org.apache.log4j.Logger;

import com.ikaver.aagarwal.hw3.common.definitions.Definitions;
import com.ikaver.aagarwal.hw3.common.mrmap.IMapInstanceRunner;
import com.ikaver.aagarwal.hw3.common.nodemanager.IMRNodeManager;
import com.ikaver.aagarwal.hw3.common.nodemanager.NodeState;
import com.ikaver.aagarwal.hw3.common.workers.MapWorkDescription;
import com.ikaver.aagarwal.hw3.common.workers.MapperChunk;
import com.ikaver.aagarwal.hw3.common.workers.MapperOutput;
import com.ikaver.aagarwal.hw3.common.workers.ReduceWorkDescription;
import com.ikaver.aagarwal.hw3.common.workers.WorkerState;

/**
 * A task manager manages a "slave" node. Following are the responsibilities of
 * a task manager.
 * 1. Accepts a jar and forks out separate jvms for running mapper and reducer
 *   tasks.
 * 2. Periodically updates master with the status of the map reduce
 *   job assigned to it.
 */
public class MRNodeManager extends UnicastRemoteObject implements IMRNodeManager  {

	protected MRNodeManager() throws RemoteException {
		super();
	}

	private static final long serialVersionUID = 1674990898801584371L;
	private static final Logger LOGGER = Logger.getLogger(MRNodeManager.class);

	/**
	 * Following is the sequence of operations which should be executed by the doMap function.
	 * 1. Fetch data from DFS and copy to the local disk.
	 * 2. Copy the jars from the DFS to the local disk.
	 * 3. Pass the local path from (1) and (2) to the mapper task.
	 * @return 
	 */
	// TODO(ankit): Return a failure error code.
	@SuppressWarnings("resource")
	public boolean doMap(MapWorkDescription input) {
		int port = MRMapTaskAttempt.startMapTask(input);
		LOGGER.info(String.format("Starting map runner at port: %d", port));
		try {
			IMapInstanceRunner runner =
					(IMapInstanceRunner) Naming.lookup(String.format("//%s:%d/%s",
					"localhost",
					port,
					Definitions.MR_MAP_RUNNER_SERVICE));

			runner.runMapInstance(input);
			
			// killing runner now!
			runner.die();

		} catch (MalformedURLException e) {
		  return false;
		} catch (RemoteException e) {
		  return false;
		} catch (NotBoundException e) {
		  return false;
		}
    return true;
	}

	public boolean doReduce(ReduceWorkDescription input) throws RemoteException {
		throw new UnsupportedOperationException("Not yet implemented :(");
	}

  public <K extends Serializable & Comparable<K>, V extends Serializable> List<MapperOutput<K, V>> dataForJob(
      int jobID, MapperChunk chunk, int reducerID) {
    throw new UnsupportedOperationException("Not yet implemented :(");
  }

  public NodeState getNodeState() {
    throw new UnsupportedOperationException("Not yet implemented :(");
  }

  public WorkerState getMapperState(int jobId, int partitionId) {
    throw new UnsupportedOperationException("Not yet implemented :(");
  }

  public WorkerState getReducerState(int jobId, int reducerId) {
    throw new UnsupportedOperationException("Not yet implemented :(");
  }
}
