package com.ikaver.aagarwal.hw3.mrmaster.scheduler;

import java.rmi.RemoteException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;

import org.apache.log4j.Logger;

import com.ikaver.aagarwal.hw3.common.nodemanager.IMRNodeManager;
import com.ikaver.aagarwal.hw3.common.nodemanager.NodeManagerFactory;
import com.ikaver.aagarwal.hw3.common.nodemanager.NodeState;
import com.ikaver.aagarwal.hw3.common.util.SocketAddress;

public class NodeTracker implements Runnable {

  private static final Logger LOG = Logger.getLogger(NodeTracker.class);

  private Map<SocketAddress, NodeInformation> nodeInfo;
  private Set<SocketAddress> allNodes;
  private Set<SocketAddress> failedNodes;
  private ReadWriteLock nodeInfoLock;

  public NodeTracker(Map<SocketAddress, NodeInformation> nodeInfo,
      Set<SocketAddress> allNodes,
      ReadWriteLock nodeInfoLock) {
    this.nodeInfo = nodeInfo;
    this.allNodes = allNodes;
    this.nodeInfoLock = nodeInfoLock;
    this.failedNodes = new HashSet<SocketAddress>();
  }

  public void run() {
    this.getStateFromNodeManagers();
  }

  private void getStateFromNodeManagers() {
    this.nodeInfoLock.writeLock().lock();
    try{
      for(SocketAddress addr : allNodes) {
        IMRNodeManager nm = NodeManagerFactory.nodeManagerFromSocketAddress(addr);
        if(nm == null) {
          nodeInfo.remove(addr);
          failedNodes.add(addr);
        }
        else {
          try {
            NodeState state = nm.getNodeState();
            if(state != null) {
              nodeInfo.put(addr, new NodeInformation(addr, 
                  state.getNumProcessors(), state.getAvailableSlots()));
              if(failedNodes.contains(addr)) {
                failedNodes.remove(addr);
              }
            }
            else {
              nodeInfo.remove(addr);
              failedNodes.add(addr);
            } 
          } catch (RemoteException e) {
            if(!failedNodes.contains(addr)) {
              LOG.warn("Failed to communicate with NM " + addr, e);
            }
            nodeInfo.remove(addr);
            failedNodes.add(addr);
          }
        }
      }
    }
    finally {
      this.nodeInfoLock.writeLock().unlock();
    }
  }

}
