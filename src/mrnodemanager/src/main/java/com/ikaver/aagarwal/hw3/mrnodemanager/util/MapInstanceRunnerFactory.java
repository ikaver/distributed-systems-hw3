package com.ikaver.aagarwal.hw3.mrnodemanager.util;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.ikaver.aagarwal.hw3.common.definitions.Definitions;
import com.ikaver.aagarwal.hw3.common.mrmap.IMapInstanceRunner;
import com.ikaver.aagarwal.hw3.common.util.SocketAddress;

public class MapInstanceRunnerFactory {
  
  private static final Logger LOG = LogManager.getLogger(MapInstanceRunnerFactory.class);

  public static IMapInstanceRunner mapRunnerFromSocketAddress(SocketAddress addr) {
    if(addr == null) return null;
    String url = String.format(
        "//%s:%d/%s", 
        addr.getHostname(), 
        addr.getPort(),
        Definitions.MR_MAP_RUNNER_SERVICE
        );
    try {
      return (IMapInstanceRunner) Naming.lookup (url);
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
