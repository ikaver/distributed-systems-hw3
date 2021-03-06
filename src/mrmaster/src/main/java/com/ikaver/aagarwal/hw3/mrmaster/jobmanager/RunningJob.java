package com.ikaver.aagarwal.hw3.mrmaster.jobmanager;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import com.ikaver.aagarwal.hw3.common.config.JobConfig;
import com.ikaver.aagarwal.hw3.common.workers.WorkerState;
import com.ikaver.aagarwal.hw3.mrmaster.scheduler.MapperWorkerInfo;
import com.ikaver.aagarwal.hw3.mrmaster.scheduler.ReducerWorkerInfo;

/**
 * Describes a job that is currently executing on the MR system.
 */
public class RunningJob {
  
  private int jobID;
  private String jobName;
  
  /**
   * Contains the mapper workers that have finished and have been committed
   */
  private Set<MapperWorkerInfo> finishedMappers;
  /**
   * Contains the reducer workers that have finished and have been committed
   */
  private Set<ReducerWorkerInfo> finishedReducers;
    
  private Set<MapperWorkerInfo> mappers;
  private Set<ReducerWorkerInfo> reducers;
  private int numFailures;
  private int numMappers;
  private int numReducers;
  private boolean mappersFinished;
  private JobConfig jobConfig;
  private ExecutorService taskTrackerService;

  public RunningJob(int jobID, String jobName, ExecutorService service,
      int numMappers, int numReducers, JobConfig jobConfig) {
    this.jobID = jobID;
    this.jobName = jobName;
    this.mappers = new HashSet<MapperWorkerInfo>();
    this.reducers = new HashSet<ReducerWorkerInfo>();
    this.finishedMappers = new HashSet<MapperWorkerInfo>();
    this.finishedReducers = new HashSet<ReducerWorkerInfo>();
    this.taskTrackerService = service;
    this.jobConfig = jobConfig;
    this.numMappers = numMappers;
    this.numReducers = numReducers;
    this.numFailures = 0;
    this.mappersFinished = false;
  }
  
  public Set<MapperWorkerInfo> getFinishedMappers() {
    return finishedMappers;
  }

  public Set<ReducerWorkerInfo> getFinishedReducers() {
    return finishedReducers;
  }

  public void shutdown() {
    taskTrackerService.shutdown();
  }
  
  public int getJobID() {
    return jobID;
  }

  public String getJobName() {
    return jobName;
  }

  public Set<MapperWorkerInfo> getMappers() {
    return mappers;
  }

  public Set<ReducerWorkerInfo> getReducers() {
    return reducers;
  }
  
  public int getAmountOfMappers() {
    return this.numMappers;
  }
  
  public int getAmountOfReducers() {
    return this.numReducers;
  }
  
  public JobConfig getJobConfig() {
    return this.jobConfig;
  }
  
  public int getNumFailures() {
    return numFailures;
  }
  
  public void setNumFailures(int numFailures) {
    this.numFailures = numFailures;
  }
  
  public void setMappersFinished(boolean mappersFinished) {
    this.mappersFinished = mappersFinished;
  }
  
  public boolean getMappersFinished() {
    return mappersFinished;
  }
  
  public int getAmountOfFinishedMappers() {
    int numMappersCompleted = 0;
    for(MapperWorkerInfo info : getMappers()) {
      if(info.getState() == WorkerState.FINISHED) {
        ++numMappersCompleted;
      }
    }
    return numMappersCompleted;
  }
  
  public int getAmountOfFinishedReducers() {
    int numReducersCompleted = 0;
    for(ReducerWorkerInfo info : getReducers()) {
      if(info.getState() == WorkerState.FINISHED) {
        ++numReducersCompleted;
      }
    }
    return numReducersCompleted;
  }
  
}
