package com.ikaver.aagarwal.hw3.mrmaster.jobmanager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.ikaver.aagarwal.hw3.common.config.FinishedJob;

/**
 * Holds the state of all of the running jobs. All of its operations are protected
 * by a lock. 
 */
public class JobsState {

  private Map<Integer, RunningJob> jobIDToJob;
  private List<FinishedJob> finishedJobs;
  private ReadWriteLock lock;
  
  public JobsState() {
    this.jobIDToJob = new HashMap<Integer, RunningJob>();
    this.finishedJobs = new ArrayList<FinishedJob>();
    this.lock = new ReentrantReadWriteLock();
  }
  
  public RunningJob getJob(int jobID) {
    RunningJob job = null;
    this.lock.readLock().lock();
    job = this.jobIDToJob.get(jobID);
    this.lock.readLock().unlock();
    return job;
  }
  
  public void addJob(RunningJob job) {
    this.lock.writeLock().lock();
    jobIDToJob.put(job.getJobID(), job);
    this.lock.writeLock().unlock();
  }
  
  public void onJobFinished(int jobID, boolean success) {
    this.lock.writeLock().lock();
    RunningJob job = jobIDToJob.get(jobID);
    if(job != null) {
      FinishedJob finishedJob = new FinishedJob(jobID, 
          job.getJobName(), success, job.getJobConfig().getOutputFilePath(),
          job.getAmountOfReducers());
      jobIDToJob.remove(jobID);
      finishedJobs.add(finishedJob);
    }
    this.lock.writeLock().unlock();
  }
  
  public List<RunningJob> currentlyRunningJobs() {
    this.lock.readLock().lock();
    List<RunningJob> jobs = new ArrayList<RunningJob>(jobIDToJob.values());
    this.lock.readLock().unlock();
    return jobs;
  }
  
  public List<FinishedJob> finishedJobs() {
    this.lock.readLock().lock();
    List<FinishedJob> finished = new ArrayList<FinishedJob>(this.finishedJobs);
    this.lock.readLock().unlock();
    return finished;
  } 

}
