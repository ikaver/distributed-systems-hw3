package com.ikaver.aagarwal.hw3.common.dfs;

import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface IDataNode extends Remote {
  
  public long sizeOfFileInBytes(String filePath) throws IOException, RemoteException;
  public byte [] getFile(String filePath) throws IOException, RemoteException;
  public void saveFile(String filePath, byte [] file) throws IOException, RemoteException;
  
}
