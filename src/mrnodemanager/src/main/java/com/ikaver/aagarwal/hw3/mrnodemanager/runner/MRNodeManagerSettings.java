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
	
  @Parameter(names = "-config", description = "System config file path", required = true)
  private String configFilePath;
  
  public String getConfigFilePath() {
    return configFilePath;
  }

	public int getPort() {
		return this.port;
	}
	
}
