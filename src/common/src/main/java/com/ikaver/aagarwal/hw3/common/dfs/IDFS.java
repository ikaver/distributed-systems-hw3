package com.ikaver.aagarwal.hw3.common.dfs;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Set;

import com.ikaver.aagarwal.hw3.common.util.SocketAddress;

public interface IDFS extends Remote {
  
  public boolean containsFile(String filePath) throws RemoteException;
  public long sizeOfFileInBytes(String filePath) throws RemoteException;
  public Set<SocketAddress> dataNodeForFile(String filePath) throws RemoteException;
  public boolean saveFile(String filePath, byte [] file) throws RemoteException;
  
}
