package com.ikaver.aagarwal.hw3.common.examples;

import com.ikaver.aagarwal.hw3.common.mrcollector.ICollector;
import com.ikaver.aagarwal.hw3.common.workers.IMapper;

/**
 * Mapper class for word count.
 */
public class WordCountMapper implements IMapper {

	public void map(String record, ICollector collector) {
		collector.collect(record, "1");
	}

}
