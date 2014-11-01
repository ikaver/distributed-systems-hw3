package com.ikaver.aagarwal.hw3.common.slave;

import java.rmi.Remote;
import java.rmi.RemoteException;

import com.ikaver.aagarwal.hw3.common.config.MRMapTaskInput;

public interface IMRTaskManager extends Remote {

	public void doMap(MRMapTaskInput input) throws RemoteException;
	
	public void doReduce() throws RemoteException;
}
