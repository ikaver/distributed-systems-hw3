package com.ikaver.aagarwal.hw3.common.mrmap;

import java.rmi.Remote;
import java.rmi.RemoteException;

import com.ikaver.aagarwal.hw3.common.workers.MapWorkDescription;
import com.ikaver.aagarwal.hw3.common.workers.WorkerState;

public interface IMapInstanceRunner extends Remote {

	public void runMapInstance(MapWorkDescription input) throws RemoteException;

	/**
	 * Enum indicating state of the mapper task.
	 */
	public WorkerState getMapperState();
	/**
	 * Die! Kill yourself.
	 */
	public void die() throws RemoteException;
	
	/**
	 * Path of the local file where the output of the map task has been dumped.
	 */
	public String getMapOutputFilePath();
}
