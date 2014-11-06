package com.ikaver.aagarwal.hw3.common.mrmap;

import java.rmi.Remote;
import java.rmi.RemoteException;

import com.ikaver.aagarwal.hw3.common.workers.MapWorkDescription;

public interface IMapInstanceRunner extends Remote {

	public void runMapInstance(MapWorkDescription input) throws RemoteException;

	public void die() throws RemoteException;
}
