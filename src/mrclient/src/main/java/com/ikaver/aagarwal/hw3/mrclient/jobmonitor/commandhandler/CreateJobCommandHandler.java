package com.ikaver.aagarwal.hw3.mrclient.jobmonitor.commandhandler;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.log4j.Logger;

import com.ikaver.aagarwal.hw3.common.commandhandler.ICommandHandler;
import com.ikaver.aagarwal.hw3.common.config.JobConfig;
import com.ikaver.aagarwal.hw3.common.config.JobFromJSONCreator;
import com.ikaver.aagarwal.hw3.common.config.JobInfoForClient;
import com.ikaver.aagarwal.hw3.mrclient.jobmonitor.JobMonitor;

public class CreateJobCommandHandler implements ICommandHandler {

  private JobMonitor monitor;
  private static final Logger LOG = Logger.getLogger(CreateJobCommandHandler.class);
  
  public CreateJobCommandHandler(JobMonitor monitor) {
    this.monitor = monitor;
  }

  public boolean handleCommand(String[] args) {
    if(args.length < 2) return false;
    String filePath = args[1];
    if(filePath == null) return false;
    
    JobConfig job = null;
    try {
      job = JobFromJSONCreator.createJobFromJSONFile(new File(filePath));
    } catch (UnsupportedEncodingException e) {
      LOG.info(e.toString());      
      System.out.println("Failed to create job, check your config file");
    } catch (IOException e) {
      LOG.info(e.toString());
      System.out.println("Failed to create job, check your config file");
    }
    
    if(job != null) {
      JobInfoForClient jobInfo = monitor.createJob(job);
      if(jobInfo == null) {
        System.out.println("Job manager failed to create job. Make sure its running and check your config file");
      }
      else {
        System.out.println("Created job: " + jobInfo);
      }
    }
    else {
      System.out.println("Failed to create job with config file: " + filePath);
    }
    
    return true;
  }

  public String helpString() {
    return "<FILEPATH>";
  }

}
