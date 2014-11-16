package com.ikaver.aagarwal.hw3.common.workers;

import java.io.Serializable;

public class MapWorkDescription implements Serializable {

	private static final long serialVersionUID = -3931848542231053572L;

	private final int jobID;
	private final MapperChunk chunk;

	// Path of the jar file on DFS which contains user
	// written mapper and reducer classes.
	private final byte [] jarFile;

	// Fully qualified name of the mapper class.
	private final String mapperClass;

	public MapWorkDescription(int jobID, MapperChunk chunk, byte [] jarFile,
			String mapperClass) {
		if (chunk == null)
			throw new IllegalArgumentException("Chunk cannot be null");
		if (jarFile == null)
			throw new IllegalArgumentException("Jar file cannot be null");
		if (mapperClass == null)
			throw new IllegalArgumentException("Mapper class cannot be null");
		this.jobID = jobID;
		this.chunk = chunk;
		this.jarFile = jarFile;
		this.mapperClass = mapperClass;
	}

	public int getJobID() {
		return jobID;
	}

	public MapperChunk getChunk() {
		return chunk;
	}

	public byte [] getJarFile() {
		return jarFile;
	}

	public String getMapperClass() {
		return mapperClass;
	}
	
	@Override
	public String toString() {
	  return String.format("Job ID: %d, mapper chunk: %s", getJobID(), getChunk());
	}

	@Override
	public boolean equals(Object obj) {
	  if (obj == null) return false;
		if (obj instanceof MapWorkDescription) {
			MapWorkDescription work = (MapWorkDescription) obj;
			if (getJobID() != work.getJobID())
				return false;
			if (getChunk().getPartitionID() != work.getChunk().getPartitionID())
				return false;
			return true;
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return new Integer(this.getJobID()).hashCode() * 59 + new Integer(getChunk().getPartitionID()).hashCode();
	}

}
