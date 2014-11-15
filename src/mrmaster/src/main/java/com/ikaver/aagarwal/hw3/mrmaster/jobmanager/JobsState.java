package com.ikaver.aagarwal.hw3.mrmaster.jobmanager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ikaver.aagarwal.hw3.common.config.FinishedJob;

public class JobsState {

  private Map<Integer, RunningJob> jobIDToJob;
  private List<FinishedJob> finishedJobs;
  
  public JobsState() {
    this.jobIDToJob = new HashMap<Integer, RunningJob>();
    this.finishedJobs = new ArrayList<FinishedJob>();
  }
  
  public RunningJob getJob(int jobID) {
    return this.jobIDToJob.get(jobID);
  }
  
  public void addJob(RunningJob job) {
    jobIDToJob.put(job.getJobID(), job);
  }
  
  public void onJobFinished(int jobID, boolean success) {
    RunningJob job = jobIDToJob.get(jobID);
    if(job != null) {
      FinishedJob finishedJob = new FinishedJob(jobID, 
          job.getJobName(), success);
      jobIDToJob.remove(jobID);
      finishedJobs.add(finishedJob);
    }
  }
  
  public List<RunningJob> currentlyRunningJobs() {
    return new ArrayList<RunningJob>(jobIDToJob.values());
  }
  
  public List<FinishedJob> finishedJobs() {
    return this.finishedJobs;
  } 

}
