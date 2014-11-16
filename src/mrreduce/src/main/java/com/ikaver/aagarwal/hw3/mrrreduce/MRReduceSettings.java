package com.ikaver.aagarwal.hw3.mrrreduce;

import com.beust.jcommander.Parameter;
import com.ikaver.aagarwal.hw3.common.workers.flags.MRWorkerRunnerSettings;

public class MRReduceSettings extends MRWorkerRunnerSettings {

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
}
