package com.ikaver.aagarwal.hw3.mrmaster.jobmanager;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class JobsState {

  private Map<Integer, RunningJob> jobIDToJob;
  
  public RunningJob getJob(int jobID) {
    return this.jobIDToJob.get(jobID);
  }
  
  public void addJob(int jobID) {
    RunningJob newJob = new RunningJob(jobID);
    jobIDToJob.put(jobID, newJob);
  }
  
  public void onJobFinished(int jobID) {
    jobIDToJob.remove(jobID);
  }
  
  public List<RunningJob> currentlyRunningJobs() {
    return new ArrayList<RunningJob>(jobIDToJob.values());
  }

}
