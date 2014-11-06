package com.ikaver.aagarwal.hw3.common.slave;

import java.rmi.Remote;
import java.rmi.RemoteException;

import com.ikaver.aagarwal.hw3.common.workers.MapWorkDescription;
import com.ikaver.aagarwal.hw3.common.workers.ReduceWorkDescription;

public interface IMRNodeManager extends Remote {

	public void doMap(MapWorkDescription input) throws RemoteException;
	
	public void doReduce(ReduceWorkDescription input) throws RemoteException;
	
	//TODO: state query methods, and other stuff
	
}