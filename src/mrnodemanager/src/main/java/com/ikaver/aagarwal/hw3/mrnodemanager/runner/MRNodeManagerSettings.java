package com.ikaver.aagarwal.hw3.mrnodemanager.runner;

import com.beust.jcommander.Parameter;

/**
 * Flags for configuring a map reduce task. A task in mr is also referred to as
 * "slave".
 */
public class MRNodeManagerSettings {

	@Parameter(names = "-port",
	    description = "Port at which the mr task remote object is bound", required = true)
	private int port;
	
	@Parameter(names = "-masterIP",
		    description = "Port at which master is running", required = true)
	private String masterIP;
	
	@Parameter(names = "-masterPort",
		    description = "Port at which master is running", required = true)
	private int masterPort;

	public int getPort() {
		return this.port;
	}
	
	public String getMasterIP() {
		return masterIP;
	}
	
	public int getMasterPort() {
		return masterPort;
	}
}
