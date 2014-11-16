package com.ikaver.aagarwal.hw3.mrclient.jobmonitor;

import java.rmi.RemoteException;
import java.util.List;

import org.apache.log4j.Logger;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.ikaver.aagarwal.hw3.common.config.FinishedJob;
import com.ikaver.aagarwal.hw3.common.config.JobConfig;
import com.ikaver.aagarwal.hw3.common.config.JobInfoForClient;
import com.ikaver.aagarwal.hw3.common.definitions.Definitions;
import com.ikaver.aagarwal.hw3.common.master.IJobManager;
import com.ikaver.aagarwal.hw3.common.util.SocketAddress;


public class JobMonitor {

  private static final Logger LOG = Logger.getLogger(JobMonitor.class);
  private IJobManagerFactory factory;
  private SocketAddress masterAddr;

  @Inject
  public JobMonitor(IJobManagerFactory factory,
      @Named(Definitions.MASTER_SOCKET_ADDR_ANNOTATION) SocketAddress masterAddr) { 
    this.factory = factory;
    this.masterAddr = masterAddr;
  }

  public JobInfoForClient createJob(JobConfig job) {
    IJobManager manager = this.factory.getJobManager(masterAddr);
    if(manager == null) return null;

    JobInfoForClient info = null;
    try {
      info = manager.createJob(job);
    } catch (RemoteException e) {
      LOG.info("Failed to create job", e);
    }
    return info;
  }

  public List<JobInfoForClient> listJobs() {
    IJobManager manager = this.factory.getJobManager(masterAddr);
    if(manager == null) return null;

    List<JobInfoForClient> jobs = null;
    try {
      jobs = manager.listJobs();
    } catch (RemoteException e) {
      LOG.info("Failed listing jobs", e);
    }
    return jobs;
  }
  
  public List<FinishedJob> listFinishedJobs() {
    IJobManager manager = this.factory.getJobManager(masterAddr);
    if(manager == null) return null;

    List<FinishedJob> jobs = null;
    try {
      jobs = manager.finishedJobs();
    } catch (RemoteException e) {
      LOG.info("Failed getting finished jobs", e);
    }
    return jobs;
  }

  public boolean terminate(int jobID) {
    IJobManager manager = this.factory.getJobManager(masterAddr);
    if(manager == null) return false;

    boolean success = false;
    try {
      success = manager.terminate(jobID);
    } catch (RemoteException e) {
      LOG.info("Failed terminating job", e);
    }
    return success;
  }
  
  public boolean shutdown() {
    IJobManager manager = this.factory.getJobManager(masterAddr);
    if(manager == null) return false;
    
    try {
      manager.shutdown();
    } catch (RemoteException e) {
      LOG.info("Failed shutting down system", e);
      return false;
    }
    return true;
  }

  public JobInfoForClient getJobInfo(int jobID) {
    IJobManager manager = this.factory.getJobManager(masterAddr);
    if(manager == null) return null;

    JobInfoForClient info = null;
    try {
      info = manager.getJobInfo(jobID);
    } catch (RemoteException e) {
      LOG.info("Failed getting job info", e);
    }
    return info;
  }  

}
