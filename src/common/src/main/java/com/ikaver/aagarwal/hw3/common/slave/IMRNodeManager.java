package com.ikaver.aagarwal.hw3.common.slave;

import java.rmi.Remote;
import java.rmi.RemoteException;

import com.ikaver.aagarwal.hw3.common.workers.MapWorkDescription;
import com.ikaver.aagarwal.hw3.common.workers.MapperChunk;
import com.ikaver.aagarwal.hw3.common.workers.ReduceWorkDescription;

public interface IMRNodeManager extends Remote {

	public void doMap(MapWorkDescription input) throws RemoteException;
	
	public void doReduce(ReduceWorkDescription input) throws RemoteException;
	
	public byte [] dataForJob(int jobID, MapperChunk chunk, int reducerID);
	
	//TODO: state query methods, and other stuff
	
}