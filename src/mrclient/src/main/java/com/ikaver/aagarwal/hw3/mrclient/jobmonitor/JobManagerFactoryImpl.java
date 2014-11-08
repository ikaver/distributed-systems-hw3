package com.ikaver.aagarwal.hw3.mrclient.jobmonitor;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.ikaver.aagarwal.hw3.common.definitions.Definitions;
import com.ikaver.aagarwal.hw3.common.master.IJobManager;
import com.ikaver.aagarwal.hw3.common.util.SocketAddress;

public class JobManagerFactoryImpl implements IJobManagerFactory {
  
  private static final Logger LOG = LogManager.getLogger(JobManagerFactoryImpl.class);

  public IJobManager getJobManager(SocketAddress addr) {
    if(addr == null) return null;
    String url = String.format(
        "//%s:%d/%s", 
        addr.getHostname(), 
        addr.getPort(),
        Definitions.JOB_MANAGER_SERVICE
    );
    try {
      return (IJobManager) Naming.lookup (url);
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

