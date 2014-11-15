package com.ikaver.aagarwal.hw3.mrclient.fileuploader;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.rmi.RemoteException;

import org.apache.log4j.Logger;

import com.ikaver.aagarwal.hw3.common.definitions.Definitions;
import com.ikaver.aagarwal.hw3.common.dfs.FileUtil;
import com.ikaver.aagarwal.hw3.common.dfs.IDFS;
import com.ikaver.aagarwal.hw3.common.util.SocketAddress;
import com.ikaver.aagarwal.hw3.mrdfs.master.DFSFactory;

public class FileUploader {
  
  private static final Logger LOG = Logger.getLogger(FileUploader.class);
  
  public static boolean uploadFile(SocketAddress dfsAddr, String filePath, String destinationPath, int recordSize) {
    IDFS dfs = DFSFactory.dfsFromSocketAddress(dfsAddr);
    if(dfs == null) {
      System.out.println("Failed to communicate with DFS");
      return false;
    }
    File file = new File(filePath);
    if(!file.exists()) {
      System.out.println("Input file does not exist.");
      return false;
    }
    if(file.isDirectory()) {
      System.out.println("Input file should be a text file, not a directory");
      return false;
    }
    long size = file.length();
    if(size % recordSize != 0) {
      System.out.println("Record size should be a divisor of total file size" + size + " " + recordSize);
      return false;
    }
    
    FileInputStream fis = null;
    try {
      fis = new FileInputStream(file);
    } catch (IOException e) {
      System.out.println("Failed to open file " + file);
      LOG.warn("Failed to open file " + file, e);
      return false;
    }
    
    try {
      if(dfs.createFile(destinationPath, recordSize, size)) {
        int numChunks = FileUtil.numChunksForFile(Definitions.SIZE_OF_CHUNK, recordSize, size);
        int recordsPerChunk = FileUtil.numRecordsPerChunk(Definitions.SIZE_OF_CHUNK, recordSize);
        int totalRecords = (int)FileUtil.getTotalRecords(recordSize, size);
        int recordNum = 0;
        byte [] data = new byte[recordsPerChunk * recordSize];
        for(int i = 0; i < numChunks; ++i) {
          System.out.printf("Uploading chunk %d of %d...\n", i+1, numChunks); 
          int len = recordSize * recordsPerChunk;
          if(i == numChunks-1) {
            len = (totalRecords - recordNum) * recordSize;
            data = new byte[len];
          }
          fis.read(data, 0, len);
          dfs.saveFile(destinationPath, i, data);
          recordNum += recordsPerChunk;
        }
        System.out.println("File uploaded successfully!");
      }
    } catch (RemoteException e) {
      System.out.println("Failed to communicate with DFS");
      LOG.warn("Failed to communicate to DFS", e);
      return false;
    } catch (IOException e) {
      System.out.println("Failed to read file " + file);
      LOG.warn("Failed to read file " + file, e);
      return false;
    }
    return true;
  }

}
