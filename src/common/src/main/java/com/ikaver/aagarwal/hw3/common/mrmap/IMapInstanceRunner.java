package com.ikaver.aagarwal.hw3.common.mrmap;

import java.rmi.Remote;
import java.rmi.RemoteException;

import com.ikaver.aagarwal.hw3.common.config.MRMapTaskInput;

public interface IMapInstanceRunner extends Remote {

	public void runMapInstance(MRMapTaskInput input) throws RemoteException;

	public void die() throws RemoteException;
}
