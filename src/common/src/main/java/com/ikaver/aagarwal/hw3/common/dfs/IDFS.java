package com.ikaver.aagarwal.hw3.common.dfs;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * The DFS master is responsible for listening for write requests of nodes. 
 * Whenever a node requests the DFS master to write a file, the DFS saves the 
 * file in several data nodes (the amount of nodes is given by the replication
 *  factor parameter) and keeps track of which node has each file. 
 *  Whenever a data node goes down, the DFS master replicates all of the files 
 *  that were on the node to the other data nodes currently on the system. 
 *  Additionally, the DFS master also responds for "name" requests, basically, 
 *  it tells the clients in which node can they find the file that they're 
 *  looking for.
 */
public interface IDFS extends Remote {
  
  public FileMetadata getMetadata(String file) throws RemoteException;
  public boolean createFile(String filePath, int recordSize, long totalFileSize) throws RemoteException;
  public boolean saveFile(String filePath, int numChunk, byte [] file) throws RemoteException;
  public boolean containsFile(String filePath) throws RemoteException;
  public byte [] getFile(String filePath, int numChunk) throws RemoteException;
  
}
