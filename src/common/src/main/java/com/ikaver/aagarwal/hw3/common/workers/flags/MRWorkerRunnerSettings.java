package com.ikaver.aagarwal.hw3.common.workers.flags;

import com.beust.jcommander.Parameter;

/**
 * Flags for starting up the map runner.
 */
public class MRWorkerRunnerSettings {

	@Parameter(names = "-port", description = "Port at which the mr task remote object is bound", required = true)
	private int port;

	public int getPort() {
		return this.port;
	}

}
