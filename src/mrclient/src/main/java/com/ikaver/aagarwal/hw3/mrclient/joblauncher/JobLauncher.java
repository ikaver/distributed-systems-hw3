package com.ikaver.aagarwal.hw3.mrclient.joblauncher;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.rmi.RemoteException;

import org.apache.log4j.Logger;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.ikaver.aagarwal.hw3.common.config.Job;
import com.ikaver.aagarwal.hw3.common.config.JobFromJSONCreator;
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
  
  public static void main(String [] args) throws UnsupportedEncodingException, IOException {
    JobLauncherSettings settings = new JobLauncherSettings();
    JCommander argsParser = new JCommander(settings);
    try {
      argsParser.parse(args);
    }
    catch (ParameterException ex) {
      argsParser.usage();
      System.exit(-1);
    }
    Injector injector = Guice.createInjector(new JobLauncherModule());
    String filePath = settings.getConfigurationFilePath();
    Job job = JobFromJSONCreator.createJobFromJSONFile(new File(filePath));
    System.out.println("Got job: " + job);
    JobLauncher launcher = injector.getInstance(JobLauncher.class);
    JobInfo info = launcher.launchJob(job);
    System.out.println("Got job info: " + info);
  }
  
}
