package com.ikaver.aagarwal.hw3.common.dfs;

import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 *  The data node is a component of the DFS module of the system. 
 *  The data node simply stores files the the DFS master requests him to store. 
 *  Additionally, it listens for read requests of other nodes. Whenever a read
 *   request comes, the data node streams the data to the node that made the 
 *   request.
 */
public interface IDataNode extends Remote {
  
  public byte [] getFile(String filePath, int numChunk) throws IOException, RemoteException;
  public void saveFile(String filePath, int numChunk, byte [] file) throws IOException, RemoteException;
  public boolean alive() throws RemoteException;
}
