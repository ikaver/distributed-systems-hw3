package com.ikaver.aagarwal.hw3.common.nodemanager;

import java.io.Serializable;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

import com.ikaver.aagarwal.hw3.common.workers.MapWorkDescription;
import com.ikaver.aagarwal.hw3.common.workers.MapperChunk;
import com.ikaver.aagarwal.hw3.common.workers.MapperOutput;
import com.ikaver.aagarwal.hw3.common.workers.ReduceWorkDescription;

public interface IMRNodeManager extends Remote {

	public void doMap(MapWorkDescription input) throws RemoteException;
	
	public void doReduce(ReduceWorkDescription input) throws RemoteException;
	
	public <K extends Serializable & Comparable<K>, V extends Serializable> 
	List<MapperOutput<K, V>> dataForJob(int jobID, MapperChunk chunk, int reducerID);
		
	//TODO: state query methods, and other stuff
	
}