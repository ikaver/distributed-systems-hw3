package com.ikaver.aagarwal.hw3.mrnodemanager.runner;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.ikaver.aagarwal.hw3.common.definitions.Definitions;
import com.ikaver.aagarwal.hw3.common.mrmap.IMapInstanceRunner;

/**
 * MRMapFactory returns the map instance runner running on the localhost
 * corresponding to the port.
 * @author ankit
 */
public class MRMapFactory {

  private static final Logger LOG = LogManager.getLogger(MRMapFactory.class);

  public static IMapInstanceRunner mapInstanceFromPort(int port) {
    if(port <= 0) {
    	LOG.warn("Invalid value of port was supplied to the factory:");
    	return null;
    }
    String url = String.format(
        "//%s:%d/%s", 
        "localhost", 
        port,
        Definitions.MR_MAP_RUNNER_SERVICE);
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
