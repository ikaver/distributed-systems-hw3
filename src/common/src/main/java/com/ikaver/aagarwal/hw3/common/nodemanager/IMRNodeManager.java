package com.ikaver.aagarwal.hw3.common.nodemanager;

import java.io.Serializable;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

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
	 * 
	 * @param jobID
	 * @param chunk
	 * @param reducerID
	 * @return
	 */
	public <K extends Serializable & Comparable<K>, V extends Serializable> List<MapperOutput<K, V>> dataForJob(
			int jobID, MapperChunk chunk, int reducerID) throws RemoteException;

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
	public WorkerState getReducerState(int jobId, int reducerId) throws RemoteException;
	
	public int getAvailableSlots() throws RemoteException;
}