package com.ikaver.aagarwal.hw3.mrdfs.master;

import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.ikaver.aagarwal.hw3.common.definitions.Definitions;
import com.ikaver.aagarwal.hw3.common.dfs.IDFS;
import com.ikaver.aagarwal.hw3.common.dfs.IDataNode;
import com.ikaver.aagarwal.hw3.common.util.SocketAddress;
import com.ikaver.aagarwal.hw3.mrdfs.datanode.DataNodeFactory;

public class DFSImpl extends UnicastRemoteObject implements IDFS {
  
  private static final long serialVersionUID = 5494800394142393419L;

  private static final Logger LOG = LogManager.getLogger(DFSImpl.class);

  private Set<SocketAddress> dataNodes;
  private Map<String, Set<SocketAddress>> filePathToDataNodes;
  private ReadWriteLock mapLock;
  private int replicationFactor;

  @Inject
  public DFSImpl(
      @Named(Definitions.DFS_REPLICATION_FACTOR_ANNOTATION) Integer replicationFactor, 
      @Named(Definitions.DFS_MAP_PATH_TO_FILE_ANNOTATION) Map<String, Set<SocketAddress>> filePathToDataNodes,
      @Named(Definitions.DFS_DATA_NODES_ANNOTATION) Set<SocketAddress> dataNodes,
      @Named(Definitions.DFS_MAP_LOCK_ANNOTATION) ReadWriteLock mapLock) throws RemoteException {
    super();
    this.filePathToDataNodes = filePathToDataNodes;
    this.replicationFactor = replicationFactor;
    this.dataNodes = dataNodes;
    this.mapLock = mapLock;
  }

  public Set<SocketAddress> dataNodeForFile(String filePath) {
    this.mapLock.readLock().lock();
    Set<SocketAddress> dataNodes = this.filePathToDataNodes.get(filePath);
    this.mapLock.readLock().unlock();
    return dataNodes;
  }

  public boolean containsFile(String filePath) throws RemoteException {
    boolean contains = false;
    this.mapLock.readLock().lock();
    contains = this.filePathToDataNodes.containsKey(filePath);
    this.mapLock.readLock().unlock();
    return contains;
  }

  public boolean saveFile(String filePath, byte[] file) {
    boolean saveSuccessful = false;
    if(!this.filePathToDataNodes.containsKey(filePath)) {
      this.mapLock.writeLock().lock();
      saveSuccessful = writeNewFile(filePath, file);
      this.mapLock.writeLock().unlock();
    }
    else {
      LOG.warn("Tried to update file " + filePath + " this is not supported!");
      this.mapLock.writeLock().lock();
      this.filePathToDataNodes.remove(filePath);
      saveSuccessful = writeNewFile(filePath, file);
      this.mapLock.writeLock().unlock();
    }
    return saveSuccessful;
  }
  
  public boolean createDirectory(String path) throws RemoteException {
    throw new UnsupportedOperationException("Not yet implemented :(");
  }
  
  public long sizeOfFileInBytes(String filePath) {
    long size = -1;
    this.mapLock.readLock().lock();
    if(this.filePathToDataNodes.containsKey(filePath)) {
      Set<SocketAddress> dataNodesAddr = this.filePathToDataNodes.get(filePath);
      for(SocketAddress addr : dataNodesAddr) {
        IDataNode dataNode = DataNodeFactory.dataNodeFromSocketAddress(addr);
        if(dataNode != null) {
          try {
            size = dataNode.sizeOfFileInBytes(filePath);
          } catch (RemoteException e) {
            LOG.warn("Remote exception getting size of file", e);
          } catch (IOException e) {
            LOG.warn("IO exception getting size of file", e);
          }
          if(size >= 0) break;
        }
      }
    }
    this.mapLock.readLock().unlock();
    return size;
  }
  
  /**
   * Writes the new file with path filePath and contents file.
   * Assumes that you currently have the write lock.
   * @param filePath
   * @param file
   * @return true iff write successful
   */
  private boolean writeNewFile(String filePath, byte [] file) {
    Set<SocketAddress> dataNodesForFile = dataNodesForNewFile();
    boolean saveSuccessful = writeFileInDataNodes(filePath, file, dataNodesForFile);
    if(saveSuccessful) {
      this.filePathToDataNodes.put(filePath, new HashSet<SocketAddress>());
    }
    return saveSuccessful;
  }

  private boolean writeFileInDataNodes(String filePath, byte [] file, Set<SocketAddress> dataNodes) {
    boolean savedAtLeastInOneDataNode = false;
    for(SocketAddress addr : dataNodes) {
      IDataNode dataNode = DataNodeFactory.dataNodeFromSocketAddress(addr);
      if(dataNode != null) {
        try {
          dataNode.saveFile(filePath, file);
          savedAtLeastInOneDataNode = true;
        } catch (IOException e) {
          LOG.warn("Failed to write file on data node", e);
        }
      }
    }  
    return savedAtLeastInOneDataNode;
  }

  private Set<SocketAddress> dataNodesForNewFile() {
    List<SocketAddress> dataNodesList = new ArrayList<SocketAddress>(this.dataNodes);
    Collections.shuffle(dataNodesList);
    Set<SocketAddress> subset = new HashSet<SocketAddress>();
    for(int i = 0; i < replicationFactor; ++i) {
      subset.add(dataNodesList.get(i));
    }
    return subset;
  }

}
