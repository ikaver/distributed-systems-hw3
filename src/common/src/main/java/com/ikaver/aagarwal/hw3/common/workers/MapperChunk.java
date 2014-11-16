package com.ikaver.aagarwal.hw3.common.workers;

import java.io.Serializable;

public class MapperChunk implements Serializable {

  private static final long serialVersionUID = -8812752984508806448L;

  private final String inputFilePath;
  private final int partitionID;
  private final int recordSize;
  private final int startRecord;
  private final int numberOfRecords;
  
  public MapperChunk(String inputFilePath, int partitionID, int startRecord, 
      int recordSize, int numberOfRecords) {
    this.inputFilePath = inputFilePath;
    this.partitionID = partitionID;
    this.recordSize = recordSize;
    this.startRecord = startRecord;
    this.numberOfRecords = numberOfRecords;
  }
  
  public String getInputFilePath() {
    return inputFilePath;
  }

  public int getPartitionID() {
    return partitionID;
  }
  
  public int getStartByteOffset() {
    return startRecord * recordSize;
  }
  
  public int getStartRecord() {
    return startRecord;
  }
  
  public int getRecordSize() {
    return recordSize;
  }
  
  public int getNumberOfRecords() {
    return numberOfRecords;
  }
  
  @Override
  public String toString() {
    return String.format("[Input path: %s, partition: %d]", getInputFilePath(), 
        getPartitionID());
  }
}
