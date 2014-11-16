package com.ikaver.aagarwal.hw3.mrreduce;


public class MRReducerCollector implements IMRReducerCollector {
	
	private StringBuilder outputStringBuilder;
	
	public MRReducerCollector() {
		outputStringBuilder = new StringBuilder();
	}
	
	public void collect(String key, String value) {
		outputStringBuilder.append(key + "," + value + "\n");
	}
	
	public byte[] getData() {
		return outputStringBuilder.toString().getBytes();
	}

}
