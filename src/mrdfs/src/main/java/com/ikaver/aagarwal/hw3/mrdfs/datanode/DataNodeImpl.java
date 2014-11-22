package com.ikaver.aagarwal.hw3.mrdfs.datanode;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

import com.ikaver.aagarwal.hw3.common.definitions.Definitions;
import com.ikaver.aagarwal.hw3.common.dfs.FileUtil;
import com.ikaver.aagarwal.hw3.common.dfs.IDataNode;

/**
 *  The data node is a component of the DFS module of the system. 
 *  The data node simply stores files the the DFS master requests him to store.
 *  Additionally, it listens for read requests of other nodes. 
 *  Whenever a read request comes, the data node streams the data to the node 
 *  that made the request.
 */
public class DataNodeImpl extends UnicastRemoteObject implements IDataNode {

  protected DataNodeImpl() throws RemoteException {
    super();
  }

  private static final long serialVersionUID = -3845024306730355403L;
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
		  throws RemoteException, IOException {
    String filePathForFile = filePathForFile(filePath, numChunk);
    File file = new File(filePathForFile(filePath, numChunk));
    FileOutputStream fos = new FileOutputStream(file);
    fos.write(data);
    fos.close();    
    FileUtil.changeFilePermission(filePathForFile);
  }
  
  public boolean alive() {
    return true;
  }
  
  private String filePathForFile(String filePath, int numChunk) {
    return String.format("%s%s%s%d", Definitions.BASE_DIRECTORY, filePath, CHUNK_NUM_SEPARATOR, numChunk);
  }

}
