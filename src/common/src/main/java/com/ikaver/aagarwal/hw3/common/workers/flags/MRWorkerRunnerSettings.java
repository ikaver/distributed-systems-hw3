package com.ikaver.aagarwal.hw3.common.workers.flags;

import com.beust.jcommander.Parameter;

/**
 * Flags for starting up the map runner.
 */
public class MRWorkerRunnerSettings {

	@Parameter(names = "-port", description = "Port at which the mr task remote object is bound", required = true)
	private int port;

	@Parameter(names = "-masterIP",
		    description = "Port at which master is running", required = true)
	private String masterIP;
	
	@Parameter(names = "-masterPort",
		    description = "Port at which master is running", required = true)
	private int masterPort;

	public String getMasterIP() {
		return masterIP;
	}
	
	public int getMasterPort() {
		return masterPort;
	}

	public int getPort() {
		return this.port;
	}
	

}
