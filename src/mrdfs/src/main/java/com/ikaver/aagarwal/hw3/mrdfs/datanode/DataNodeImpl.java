package com.ikaver.aagarwal.hw3.mrdfs.datanode;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.rmi.RemoteException;

import com.ikaver.aagarwal.hw3.common.definitions.Definitions;
import com.ikaver.aagarwal.hw3.common.dfs.IDataNode;

public class DataNodeImpl implements IDataNode {
  
  private static final String CHUNK_NUM_SEPARATOR = "______";

  public byte[] getFile(String filePath, int numChunk) throws IOException,
      RemoteException {
    File file = new File(filePathForFile(filePath, numChunk));
    FileInputStream fis = new FileInputStream(file);
    byte[] data = new byte[(int)file.length()];
    fis.read(data);
    fis.close();
    return data; 
  }

  public void saveFile(String filePath, int numChunk, byte[] data)
      throws IOException, RemoteException {
    File file = new File(filePathForFile(filePath, numChunk));
    FileOutputStream fos = new FileOutputStream(file);
    fos.write(data);
    fos.close();    
  }
  
  public boolean alive() {
    return true;
  }
  
  private String filePathForFile(String filePath, int numChunk) {
    return String.format("%s%s%s%d", Definitions.BASE_DIRECTORY, filePath, CHUNK_NUM_SEPARATOR, numChunk);
  }

}
