package com.ikaver.aagarwal.hw3.common.dfs;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.ikaver.aagarwal.hw3.common.definitions.Definitions;
import com.ikaver.aagarwal.hw3.common.util.SocketAddress;

/**
 * Helper factory that returns a (remote) reference to the DFS master object.
 */
public class DFSFactory {
  
  private static final Logger LOG = LogManager.getLogger(DFSFactory.class);

  public static IDFS dfsFromSocketAddress(SocketAddress addr) {
    if(addr == null) return null;
    String url = String.format(
        "//%s:%d/%s", 
        addr.getHostname(), 
        addr.getPort(),
        Definitions.DFS_SERVICE
        );
    try {
      return (IDFS) Naming.lookup (url);
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
