package com.ikaver.aagarwal.hw3.mrmaster.scheduler;

import java.rmi.RemoteException;
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
  private ReadWriteLock nodeInfoLock;

  public NodeTracker(Map<SocketAddress, NodeInformation> nodeInfo,
      Set<SocketAddress> allNodes,
      ReadWriteLock nodeInfoLock) {
    this.nodeInfo = nodeInfo;
    this.allNodes = allNodes;
    this.nodeInfoLock = nodeInfoLock;
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
        }
        else {
          try {
            NodeState state = nm.getNodeState();
            if(state != null) {
              nodeInfo.put(addr, new NodeInformation(addr, 
                  state.getNumProcessors(), state.getAvailableSlots()));
            }
            else {
              nodeInfo.remove(addr);
            } 
          } catch (RemoteException e) {
            LOG.warn("Failed to communicate with node manager", e);
            nodeInfo.remove(addr);
          }
        }
      }
    }
    finally {
      this.nodeInfoLock.writeLock().unlock();
    }
  }

}
