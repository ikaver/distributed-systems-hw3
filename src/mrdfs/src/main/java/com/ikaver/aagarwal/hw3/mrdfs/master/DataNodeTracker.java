package com.ikaver.aagarwal.hw3.mrdfs.master;

import java.rmi.RemoteException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;

import com.ikaver.aagarwal.hw3.common.dfs.IDataNode;
import com.ikaver.aagarwal.hw3.common.util.SocketAddress;
import com.ikaver.aagarwal.hw3.mrdfs.datanode.DataNodeFactory;

public class DataNodeTracker implements Runnable {
  
  private Set<SocketAddress> dataNodes;
  private ReadWriteLock dataNodesLock;
  private IOnDataNodeFailureHandler onFailureHandler;

  public void run() {
    queryDataNodes();
  }
  
  public void queryDataNodes() {
    Set<SocketAddress> unresponsiveDataNodes = new HashSet<SocketAddress>();
    this.dataNodesLock.readLock().lock();
    Set<SocketAddress> nodes = new HashSet<SocketAddress>(dataNodes);
    this.dataNodesLock.readLock().unlock();
    
    for(SocketAddress node : nodes) {
      IDataNode dataNode = DataNodeFactory.dataNodeFromSocketAddress(node);
      try {
        if(dataNode == null || !dataNode.alive()) {
          unresponsiveDataNodes.add(node);
        }
      }
      catch(RemoteException e) {
        unresponsiveDataNodes.add(node);
      }
    }
    
    this.dataNodesLock.writeLock().lock();
    this.dataNodes.removeAll(unresponsiveDataNodes);
    this.dataNodesLock.writeLock().unlock();
    if(unresponsiveDataNodes.size() > 0) {
      for(SocketAddress addr : unresponsiveDataNodes) {
        this.onFailureHandler.onDataNodeFailed(addr);
      }
    }
  }

  
}
