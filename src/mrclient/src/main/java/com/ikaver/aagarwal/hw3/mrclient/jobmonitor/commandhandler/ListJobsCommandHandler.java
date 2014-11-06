package com.ikaver.aagarwal.hw3.mrclient.jobmonitor.commandhandler;

import java.util.List;

import com.ikaver.aagarwal.hw3.common.commandhandler.ICommandHandler;
import com.ikaver.aagarwal.hw3.common.config.JobInfoForClient;
import com.ikaver.aagarwal.hw3.mrclient.jobmonitor.JobMonitor;

public class ListJobsCommandHandler implements ICommandHandler {
  
  private JobMonitor monitor;
  
  public ListJobsCommandHandler(JobMonitor monitor) {
    this.monitor = monitor;
  }

  public boolean handleCommand(String[] args) {
    List<JobInfoForClient> jobInfo = monitor.listJobs();
    if(jobInfo == null) {
      System.out.println("Failed to get list of jobs...");
    }
    else if(jobInfo.size() == 0) {
      System.out.println("No jobs currently running...");
    }
    else {
      for(JobInfoForClient info : jobInfo) {
        System.out.println(info);
      }
    }
    return true;
  }

  public String helpString() {
    return "";
  }

}
