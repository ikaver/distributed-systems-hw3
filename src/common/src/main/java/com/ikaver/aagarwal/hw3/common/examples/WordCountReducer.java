package com.ikaver.aagarwal.hw3.common.examples;

import com.ikaver.aagarwal.hw3.common.workers.IReducer;

/**
 * Reducer class for word count example.
 */
public class WordCountReducer implements IReducer {

	public void reduce() {
		System.out.println("Reduce function called.");
	}

}