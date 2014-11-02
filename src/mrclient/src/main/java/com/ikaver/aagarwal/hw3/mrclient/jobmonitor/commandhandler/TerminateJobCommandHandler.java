package com.ikaver.aagarwal.hw3.mrclient.jobmonitor.commandhandler;

import com.ikaver.aagarwal.hw3.common.commandhandler.ICommandHandler;
import com.ikaver.aagarwal.hw3.mrclient.jobmonitor.JobMonitor;

public class TerminateJobCommandHandler implements ICommandHandler {
  
  private JobMonitor monitor;
  
  public TerminateJobCommandHandler(JobMonitor monitor) {
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
    
    if(monitor.terminate(jobID)) {
      System.out.printf("Job %d terminated.\n", jobID);
    }
    else {
      System.out.printf("Failed to terminate job %d.\n", jobID);
    }
    return true;
  }

  public String helpString() {
    return "<JOBID>";
  }

}
