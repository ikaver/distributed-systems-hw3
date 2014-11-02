package com.ikaver.aagarwal.hw3.mrclient.jobmonitor;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.ikaver.aagarwal.hw3.common.definitions.Definitions;
import com.ikaver.aagarwal.hw3.common.master.IJobManager;

public class JobManagerFactoryImpl implements IJobManagerFactory {
  
  private static final Logger LOG = LogManager.getLogger(JobManagerFactoryImpl.class);

  public IJobManager getJobManager(String masterIP, int masterPort) {
    if(masterIP == null) return null;
    String url = String.format(
        "//%s:%d/%s", 
        masterIP, 
        masterPort,
        Definitions.JOB_MANAGER_SERVICE
    );
    try {
      return (IJobManager) Naming.lookup (url);
    } catch (MalformedURLException e) {
      LOG.info("Bad URL", e);
    } catch (RemoteException e) {
      LOG.info("Remote connection refused to url "+ url);
    } catch (NotBoundException e) {
      LOG.info("Not bound", e);
    }
    return null;
  }



}

