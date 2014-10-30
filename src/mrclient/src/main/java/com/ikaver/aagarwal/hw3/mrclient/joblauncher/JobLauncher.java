package com.ikaver.aagarwal.hw3.mrclient.joblauncher;

import java.rmi.RemoteException;

import org.apache.log4j.Logger;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.google.inject.Inject;
import com.ikaver.aagarwal.hw3.common.config.Job;
import com.ikaver.aagarwal.hw3.common.config.JobInfo;
import com.ikaver.aagarwal.hw3.common.master.IJobManager;

public class JobLauncher {
  
  private static final Logger LOG = Logger.getLogger(JobLauncher.class);
  private IJobManagerFactory factory;
 
  @Inject
  public JobLauncher(IJobManagerFactory factory) {
    this.factory = factory;
  }
  
  public JobInfo launchJob(Job job) {
    IJobManager manager = this.factory.jobManagerFromJob(job);
    if(manager == null) return null;
    
    JobInfo jobInfo = null;
    try {
      jobInfo = manager.createJob(job);
    } catch (RemoteException e) {
      LOG.info("Failed launching job", e);
    }
    
    return jobInfo;
  }
  
  public static void main(String [] args) {
    JobLauncherConfig.initialize();
    JobLauncherSettings settings = new JobLauncherSettings();
    JCommander argsParser = new JCommander(settings);
    try {
      argsParser.parse(args);
    }
    catch (ParameterException ex) {
      argsParser.usage();
      System.exit(-1);
    }
    
    String filePath = settings.getConfigurationFilePath();
    
  }
  
}
