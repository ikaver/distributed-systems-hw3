package com.ikaver.aagarwal.hw3.common.examples;

import com.ikaver.aagarwal.hw3.common.mrcollector.ICollector;

/**
 * A buggy word count mapper which demonstrates that our system is tolerant
 * to errors in user code.
 */
public class BuggyWordCountMapper {
	
	public void map(String record, ICollector collector) {
		String cleanRecord = record.trim();
		collector.collect(cleanRecord, "1");	
		doEvilStuff();
		
	}
	
	/**
	 * Although this is not a "pure" JVM crash, but nevertheless is enough
	 * to demonstrate the functionalities of our code.
	 */
	private void doEvilStuff() {
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			// Do nothing.
		}
		System.exit(1);
	}

}
