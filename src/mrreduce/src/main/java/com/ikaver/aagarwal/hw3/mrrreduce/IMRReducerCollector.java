package com.ikaver.aagarwal.hw3.mrrreduce;

import com.ikaver.aagarwal.hw3.common.mrcollector.ICollector;

public interface IMRReducerCollector extends ICollector {
	
	/**
	 * Data array to be written to DFS.
	 */
	public byte[] getData();

}
