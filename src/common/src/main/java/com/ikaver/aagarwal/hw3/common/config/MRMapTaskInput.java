package com.ikaver.aagarwal.hw3.common.config;

import java.io.Serializable;

/**
 * A POJO containing the list of properties which is required for running a
 * mapper task.
 * 
 * The POJO is received by the {@code MRTaskManager}.
 */
public class MRMapTaskInput implements Serializable {
	
	private static final long serialVersionUID = -224112533334384429L;

	// Path of the jar file on DFS which contains user
	// written mapper and reducer classes.
	private final String jarPath;
	
	// Fully qualified name of the mapper class.
	private final String mapperClass;
	
	public MRMapTaskInput(String jarPath,
			String mapperClass) {
		this.jarPath = jarPath;
		this.mapperClass = mapperClass;
	}
	
	public String getJarPath() {
		return jarPath;
	}
	
	public String getMapperClass() {
		return mapperClass;
	}
}
