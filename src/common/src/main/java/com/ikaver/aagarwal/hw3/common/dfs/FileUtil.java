package com.ikaver.aagarwal.hw3.common.dfs;

import java.io.IOException;

import org.apache.log4j.Logger;

public class FileUtil {

  private static final Logger LOGGER = Logger.getLogger(FileUtil.class);

  public static int numChunksForFile(int sizeOfChunk, int recordSize, long totalFileSize) {
    int recordsInChunk = numRecordsPerChunk(sizeOfChunk, recordSize);
    int numChunks = (int)(Math.ceil(totalFileSize / (double)(recordsInChunk * recordSize)));
    return numChunks;
  }
  
  public static int numRecordsPerChunk(int sizeOfChunk, int recordSize) {
    return (int)(Math.floor(sizeOfChunk / (double)(recordSize)));
  }
  
  public static long getTotalRecords(int recordSize, long totalFileSize ) {
    return totalFileSize / recordSize;
  }

  public static void changeFilePermission(String filePathForFile) {
	    try {
			Runtime.getRuntime().exec("chmod 777 " + filePathForFile);
		} catch (IOException e) {
			LOGGER.warn("error changing file permission for " + filePathForFile, e);
		}
  }

}
