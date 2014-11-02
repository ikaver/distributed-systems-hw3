package com.ikaver.aagarwal.hw3.mrclient.jobmonitor.commandhandler;

import com.ikaver.aagarwal.hw3.common.commandhandler.ICommandHandler;
import com.ikaver.aagarwal.hw3.common.config.JobInfo;
import com.ikaver.aagarwal.hw3.mrclient.jobmonitor.JobMonitor;

public class JobInfoCommandHandler implements ICommandHandler {

  private JobMonitor monitor;
  
  public JobInfoCommandHandler(JobMonitor monitor) {
    this.monitor = monitor;
  }

  public boolean handleCommand(String[] args) {
    if(args.length < 2) return false;
    
    int jobID = -1;
    try{
      jobID = Integer.parseInt(args[1]);
    }
    catch(NumberFormatException e) {
      System.out.println("Expected integer job ID.");
      return true;
    }
    JobInfo info = monitor.getJobInfo(jobID);
    if(info != null) {
      System.out.printf("Job status: %s", info);
    }
    else {
      System.out.printf("Failed to get job info for job with ID = %d.\n", jobID);
    }
    return true;
  }


  public String helpString() {
    return "<JOBID>";
  }



}
