package com.ikaver.aagarwal.hw3.common.mrmap;

import com.ikaver.aagarwal.hw3.common.mrcollector.ICollector;

public interface IMapOutputCollector extends ICollector {
	/**
	 * Write the collected map output to a file and returns a file path to the
	 * same.
	 * 
	 * @return
	 */
	public String flush();

}
