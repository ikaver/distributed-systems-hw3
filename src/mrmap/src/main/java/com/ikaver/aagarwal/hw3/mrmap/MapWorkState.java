package com.ikaver.aagarwal.hw3.mrmap;

import com.ikaver.aagarwal.hw3.common.workers.WorkerState;

/**
 * Structure encapsulating the state of the work which has been
 * performed by MR so far.
 */
public class MapWorkState {

	private String outputPath;
	private WorkerState state;
	
	public MapWorkState() {
	  this.state = WorkerState.RUNNING;
	}

	public String getOutputPath() {
		return outputPath;
	}
	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
	}
	public WorkerState getState() {
		return state;
	}
	public synchronized void setState(WorkerState state) {
		this.state = state;
	}
}
