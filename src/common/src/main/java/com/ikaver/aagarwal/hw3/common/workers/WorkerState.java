package com.ikaver.aagarwal.hw3.common.workers;


public enum WorkerState {
	/**
	 * Indicates that the worker is running and hasn't completed the task at hand.
	 */
	RUNNING,
	/**
	 * Indicates that the worker failed.
	 */
	FAILED,
	/**
	 * Indicates that the worker terminated successfully and output (if any) is ready
	 * to be read from the fs.
	 */
	FINISHED,
}
