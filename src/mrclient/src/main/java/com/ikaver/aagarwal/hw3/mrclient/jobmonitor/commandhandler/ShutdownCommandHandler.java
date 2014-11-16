package com.ikaver.aagarwal.hw3.mrclient.jobmonitor.commandhandler;

import com.ikaver.aagarwal.hw3.common.commandhandler.ICommandHandler;
import com.ikaver.aagarwal.hw3.mrclient.jobmonitor.JobMonitor;

public class ShutdownCommandHandler implements ICommandHandler {
  
  private JobMonitor monitor;
  
  public ShutdownCommandHandler(JobMonitor monitor) {
    this.monitor = monitor;
  }

  public boolean handleCommand(String[] args) {
    if(args.length < 1) return false;
    
    if(monitor.shutdown()) {
      System.out.printf("System was shutdown");
      System.exit(0);
    }
    else {
      System.out.printf("Failed to shutdown system. Try again later");
    }
    return true;
  }

  public String helpString() {
    return "";
  }

}
