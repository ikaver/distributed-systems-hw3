package com.ikaver.aagarwal.hw3.common.workers;

import com.ikaver.aagarwal.hw3.common.mrcollector.ICollector;

/**
 * Interface which must be extended by any reducer class of the map
 * reduce framework.
 */
public interface IReducer {
	public void reduce(ICollector collector);
}
