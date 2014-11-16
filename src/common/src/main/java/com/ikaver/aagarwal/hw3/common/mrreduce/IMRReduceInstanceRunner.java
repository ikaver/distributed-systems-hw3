package com.ikaver.aagarwal.hw3.common.mrreduce;

import java.rmi.Remote;
import java.rmi.RemoteException;

import com.ikaver.aagarwal.hw3.common.workers.ReduceWorkDescription;
import com.ikaver.aagarwal.hw3.common.workers.WorkerState;

/**
 * A reduce instance runner does more heavy lifting than a map instance.
 * It fetches it's data from all the mapper tasks and copies the file
 * back to DFS.
 */
public interface IMRReduceInstanceRunner extends Remote {
	/**
	 * Runs a reduce instance
	 */
	public void runReduceInstance(ReduceWorkDescription rwd) throws RemoteException;

	/**
	 * Enum indicating state of the reducer task.
	 */
	public WorkerState getReducerState() throws RemoteException;

	/**
	 * Die! Kill yourself.
	 */
	public void die() throws RemoteException;
}
