package com.ikaver.aagarwal.hw3.mrclient.jobmonitor;

import java.rmi.RemoteException;
import java.util.List;

import org.apache.log4j.Logger;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.ikaver.aagarwal.hw3.common.config.JobConfig;
import com.ikaver.aagarwal.hw3.common.config.JobInfo;
import com.ikaver.aagarwal.hw3.common.definitions.Definitions;
import com.ikaver.aagarwal.hw3.common.master.IJobManager;


public class JobMonitor {

  private static final Logger LOG = Logger.getLogger(JobMonitor.class);
  private IJobManagerFactory factory;
  private String masterIP;
  private int masterPort;

  @Inject
  public JobMonitor(IJobManagerFactory factory,
      @Named(Definitions.MASTER_IP_ANNOTATION) String masterIP, 
      @Named(Definitions.MASTER_PORT_ANNOTATION) Integer masterPort) {
    this.factory = factory;
    this.masterIP = masterIP;
    this.masterPort = masterPort;
  }

  public JobInfo createJob(JobConfig job) {
    IJobManager manager = this.factory.getJobManager(masterIP, masterPort);
    if(manager == null) return null;

    JobInfo info = null;
    try {
      info = manager.createJob(job);
    } catch (RemoteException e) {
      LOG.info("Failed to create job", e);
    }
    return info;
  }

  public List<JobInfo> listJobs() {
    IJobManager manager = this.factory.getJobManager(masterIP, masterPort);
    if(manager == null) return null;

    List<JobInfo> jobs = null;
    try {
      jobs = manager.listJobs();
    } catch (RemoteException e) {
      LOG.info("Failed listing jobs", e);
    }
    return jobs;
  }

  public boolean terminate(int jobID) {
    IJobManager manager = this.factory.getJobManager(masterIP, masterPort);
    if(manager == null) return false;

    boolean success = false;
    try {
      success = manager.terminate(jobID);
    } catch (RemoteException e) {
      LOG.info("Failed terminating job", e);
    }
    return success;
  }

  public JobInfo getJobInfo(int jobID) {
    IJobManager manager = this.factory.getJobManager(masterIP, masterPort);
    if(manager == null) return null;

    JobInfo info = null;
    try {
      info = manager.getJobInfo(jobID);
    } catch (RemoteException e) {
      LOG.info("Failed getting job info", e);
    }
    return info;
  }  

}
