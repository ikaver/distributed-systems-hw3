package com.ikaver.aagarwal.hw3.mrslave.runner;

import com.beust.jcommander.Parameter;

/**
 * Flags for configuring a map reduce task. A task in mr is also referred to as
 * "slave".
 */
public class MRTaskManagerSettings {

	@Parameter(names = "-port",
	    description = "Port at which the mr task remote object is bound", required = true)

	private String port;

	public String getPort() {
		return this.port;
	}

}
