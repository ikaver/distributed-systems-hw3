package com.ikaver.aagarwal.hw3.mrslave.main;

import java.rmi.RemoteException;

import com.ikaver.aagarwal.hw3.common.config.MRMapTaskInput;

public class MRTaskManagerEntryPoint {
	
	public static void main(String args[]) throws RemoteException {
		MRTaskManager manager = new MRTaskManager();
		MRMapTaskInput input = new MRMapTaskInput(
				"/home/ankit/git/distributed-systems-hw3/src/common/target/" +
				"common-1.0-SNAPSHOT-jar-with-dependencies.jar",
				"com.ikaver.aagarwal.hw3.common.examples.WordCountMapper");

		manager.doMap(input);
	}

}
