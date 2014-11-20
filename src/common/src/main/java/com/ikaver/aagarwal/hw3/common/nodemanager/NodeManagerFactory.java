package com.ikaver.aagarwal.hw3.common.nodemanager;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;

import org.apache.log4j.Logger;

import com.ikaver.aagarwal.hw3.common.definitions.Definitions;
import com.ikaver.aagarwal.hw3.common.nodemanager.IMRNodeManager;
import com.ikaver.aagarwal.hw3.common.util.SocketAddress;

public class NodeManagerFactory {
  
  private static final Logger LOG = Logger.getLogger(NodeManagerFactory.class);
  
  public static IMRNodeManager nodeManagerFromSocketAddress(SocketAddress addr) {
    if(addr == null) return null;
    String url = String.format(
        "//%s:%d/%s", 
        addr.getHostname(), 
        addr.getPort(),
        Definitions.MR_NODE_MANAGER_SERVICE
        );
    try {
      return (IMRNodeManager) Naming.lookup (url);
    } catch (MalformedURLException e) {
      LOG.debug("Bad URL", e);
    } catch (RemoteException e) {
      LOG.debug("Failed to communicate with node manager", e);
    } catch (NotBoundException e) {
      LOG.debug("Not bound", e);
    }
    return null;
  }

}
