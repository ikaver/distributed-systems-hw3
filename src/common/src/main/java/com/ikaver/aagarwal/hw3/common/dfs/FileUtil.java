package com.ikaver.aagarwal.hw3.common.dfs;

public class FileUtil {
  
  public static int numChunksForFile(int sizeOfChunk, int recordSize, long totalFileSize) {
    int recordsInChunk = numRecordsPerChunk(sizeOfChunk, recordSize);
    int numChunks = (int)(Math.ceil(totalFileSize / (double)(recordsInChunk)));
    return numChunks;
  }
  
  public static int numRecordsPerChunk(int sizeOfChunk, int recordSize) {
    return (int)(Math.floor(sizeOfChunk / (double)(recordSize)));
  }
  
  public static long getTotalRecords(int recordSize, long totalFileSize ) {
    return totalFileSize / recordSize;
  }

}
