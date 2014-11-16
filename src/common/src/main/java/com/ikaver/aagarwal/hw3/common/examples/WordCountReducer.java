package com.ikaver.aagarwal.hw3.common.examples;

import java.util.List;

import com.ikaver.aagarwal.hw3.common.mrcollector.ICollector;
import com.ikaver.aagarwal.hw3.common.workers.IReducer;

/**
 * Reducer class for word count example.
 */
public class WordCountReducer implements IReducer {

	public void reduce(ICollector collector, String key, List<String> values) {
		collector.collect(key, Integer.toString(values.size()));
	}
}
