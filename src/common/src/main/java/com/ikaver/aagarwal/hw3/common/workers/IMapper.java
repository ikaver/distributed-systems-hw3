package com.ikaver.aagarwal.hw3.common.workers;

import com.ikaver.aagarwal.hw3.common.mrcollector.ICollector;

/**
 * Interface which must be extended by a mapper class of the
 * map reduce framework.
 */
public interface IMapper {
	public void map(String record, ICollector collector);
}
