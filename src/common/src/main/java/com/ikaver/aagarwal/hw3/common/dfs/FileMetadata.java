package com.ikaver.aagarwal.hw3.common.dfs;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

import com.ikaver.aagarwal.hw3.common.util.SocketAddress;

/**
 * Describes the metadata of a file stored on the DFS. More importantly, it 
 * holds the socket address of all of the nodes that have stored chunks of the
 * file.
 */
public class FileMetadata implements Serializable {
  
  private static final long serialVersionUID = 7914506954246518081L;
  
  private String fileName;
  private int numChunks;
  private Map<Integer, Set<SocketAddress> > numChunkToAddr;
  private int recordSize;
  private long sizeOfFile;
  
  public FileMetadata(String fileName, int numChunks, 
      Map<Integer, Set<SocketAddress>> numChunkToAddr,
      int recordSize, long sizeOfFile) {
    if(numChunkToAddr == null) throw new IllegalArgumentException("Map cannot be null");
    this.fileName = fileName;
    this.numChunkToAddr = numChunkToAddr;
    this.numChunks = numChunks;
    this.recordSize = recordSize;
    this.sizeOfFile = sizeOfFile;
  }
  
  public String getFileName() {
    return fileName;
  }

  public int getNumChunks() {
    return numChunks;
  }
  
  public Map<Integer, Set<SocketAddress>> getNumChunkToAddr() {
    return numChunkToAddr;
  }
  
  public int getRecordSize() {
    return recordSize;
  }
  
  public long getSizeOfFile() {
    return sizeOfFile;
  }
  
  @Override
  public String toString() {
    return String.format("[Filename: %s , Num chunks: %d , Record size: %d , Size of file: %d]",
        getFileName(), getNumChunks(), getRecordSize(), getSizeOfFile());
  }

}
