package com.ikaver.aagarwal.hw3.mrdfs.datanode;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.ikaver.aagarwal.hw3.common.definitions.Definitions;
import com.ikaver.aagarwal.hw3.common.dfs.IDataNode;
import com.ikaver.aagarwal.hw3.common.util.SocketAddress;

/**
 * Helper class that returns a (remote) reference to a data node given a 
 * socket address
 */
public class DataNodeFactory {
  
  private static final Logger LOG = LogManager.getLogger(DataNodeFactory.class);

  public static IDataNode dataNodeFromSocketAddress(SocketAddress addr) {
    if(addr == null) return null;
    String url = String.format(
        "//%s:%d/%s", 
        addr.getHostname(), 
        addr.getPort(),
        Definitions.DATA_NODE_SERVICE
        );
    try {
      return (IDataNode) Naming.lookup (url);
    } catch (MalformedURLException e) {
      LOG.info("Bad URL", e);
    } catch (RemoteException e) {
      LOG.info("Remote connection refused to url "+ url, e);
    } catch (NotBoundException e) {
      LOG.info("Not bound", e);
    }
    return null;
  }

}
