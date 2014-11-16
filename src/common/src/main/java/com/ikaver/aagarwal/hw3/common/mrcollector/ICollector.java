package com.ikaver.aagarwal.hw3.common.mrcollector;

/**
 * Collect *collects* output for a map/reduce task.
 */
public interface ICollector {
	public void collect(String key, String value);
}
