package com.ikaver.aagarwal.hw3.common.nodemanager;

import java.io.Serializable;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

import com.ikaver.aagarwal.hw3.common.objects.KeyValuePair;
import com.ikaver.aagarwal.hw3.common.util.SocketAddress;
import com.ikaver.aagarwal.hw3.common.workers.MapWorkDescription;
import com.ikaver.aagarwal.hw3.common.workers.MapperChunk;
import com.ikaver.aagarwal.hw3.common.workers.MapperOutput;
import com.ikaver.aagarwal.hw3.common.workers.ReduceWorkDescription;
import com.ikaver.aagarwal.hw3.common.workers.WorkerState;

public interface IMRNodeManager extends Remote {

	/**
	 * Run some map task.
	 * @param input
	 * @return boolean indicating if the map task was successfully launched.
	 * @throws RemoteException
	 */
	public boolean doMap(MapWorkDescription input) throws RemoteException;

	/**
	 * Run some reduce task.
	 * 
	 * @param input
	 * @return boolean indicating if the map task was successfully launched.
	 * @throws RemoteException
	 */
	public boolean doReduce(ReduceWorkDescription input) throws RemoteException;
	
	public void updateMappersReferences(List<SocketAddress> mapperAddr, 
	    List<MapperChunk> chunks) throws RemoteException;
	
	/**
	 * Terminates all workers working for job with jobID (jobID).
	 * @param jobID
	 */
	public boolean terminateWorkers(int jobID) throws RemoteException;
	
	/**
	 * Fetches data from a mapper corresponding to the map work. It is assumed that this
	 * function is called only when all the mappers have finished executing.
	 * @param mwd is the map work description.
	 * @param reducerID
	 * @return
	 */
	public List<KeyValuePair> dataForJob(
			MapWorkDescription mwd, int reducerID) throws RemoteException;

	/**
	 * Returns the status and capacity of number of mappers and reducers.
	 * @return
	 */
	public NodeState getNodeState() throws RemoteException;
	
	/**
	 * Returns an enum indicating state of the mapper.
	 * @param jobId
	 * @param partitionId
	 */
	public WorkerState getMapperState(MapWorkDescription mwd) throws RemoteException;

	/**
	 * Returns an enum indicating state of the reducer.
	 * @param jobId
	 * @param reducerId
	 */
	public WorkerState getReducerState(ReduceWorkDescription workInfo) throws RemoteException;
	
}