package com.ikaver.aagarwal.hw3.common.workers;


public enum WorkerState {
  /**
   * Failed to create a worker for this task
   */
  WORKER_NOT_ASSIGNED,
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
	 * to be read from the fs. At this stage, it may still not be a good idea to kill
	 * the mapper task since it may not have informed the node manager about the 
	 * fact that it has finished it's task.
	 */
	FINISHED,
	/**
	 * Indicates if the the worker can be garbage collected.
	 */
	GARBAGE_COLLECT,
	/**
	 * Indicates that the worker whose state is being requested doesn't exist.
	 */
	WORKER_DOESNT_EXIST,
}
