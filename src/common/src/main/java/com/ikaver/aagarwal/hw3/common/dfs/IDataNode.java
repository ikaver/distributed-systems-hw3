package com.ikaver.aagarwal.hw3.common.dfs;

import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface IDataNode extends Remote {
  
  public byte [] getFile(String filePath, int numChunk) throws IOException, RemoteException;
  public void saveFile(String filePath, int numChunk, byte [] file) throws IOException, RemoteException;
  public boolean alive() throws RemoteException;
}
