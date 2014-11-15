package com.ikaver.aagarwal.hw3.mrclient.jobmonitor.commandhandler;

import java.util.List;

import com.ikaver.aagarwal.hw3.common.commandhandler.ICommandHandler;
import com.ikaver.aagarwal.hw3.common.config.FinishedJob;
import com.ikaver.aagarwal.hw3.mrclient.jobmonitor.JobMonitor;

public class ListFinishedJobsCommandHandler implements ICommandHandler {

  private JobMonitor monitor;

  public ListFinishedJobsCommandHandler(JobMonitor monitor) {
    this.monitor = monitor;
  }

  public boolean handleCommand(String[] args) {
    List<FinishedJob> finishedJobs = monitor.listFinishedJobs();
    if(finishedJobs == null) {
      System.out.println("Failed to get list of jobs...");
    }
    else if(finishedJobs.size() == 0) {
      System.out.println("No jobs have finished...");
    }
    else {
      for(FinishedJob job : finishedJobs) {
        System.out.println(job);
      }
    }
    return true;
  }

  public String helpString() {
    return "";
  }

}
