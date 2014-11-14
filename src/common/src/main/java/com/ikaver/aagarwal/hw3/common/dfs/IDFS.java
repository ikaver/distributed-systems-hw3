package com.ikaver.aagarwal.hw3.common.dfs;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface IDFS extends Remote {
  
  public FileMetadata getMetadata(String file) throws RemoteException;
  public boolean createFile(String filePath, int recordSize, long totalFileSize) throws RemoteException;
  public boolean saveFile(String filePath, int numChunk, byte [] file) throws RemoteException;
  public boolean containsFile(String filePath) throws RemoteException;
  
}
