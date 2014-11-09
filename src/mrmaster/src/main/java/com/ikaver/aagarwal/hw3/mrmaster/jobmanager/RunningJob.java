package com.ikaver.aagarwal.hw3.mrmaster.jobmanager;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import com.ikaver.aagarwal.hw3.common.workers.WorkerState;
import com.ikaver.aagarwal.hw3.mrmaster.scheduler.MapperWorkerInfo;
import com.ikaver.aagarwal.hw3.mrmaster.scheduler.ReducerWorkerInfo;

public class RunningJob {
  
  private int jobID;
  private String jobName;

  private Set<MapperWorkerInfo> mappers;
  private Set<ReducerWorkerInfo> reducers;
  private ExecutorService runningService;

  public RunningJob(int jobID, String jobName, ExecutorService service) {
    this.jobID = jobID;
    this.jobName = jobName;
    this.mappers = new HashSet<MapperWorkerInfo>();
    this.reducers = new HashSet<ReducerWorkerInfo>();
    this.runningService = service;
  }
  
  public void shutdown() {
    runningService.shutdown();
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
    return this.getMappers().size();
  }
  
  public int getAmountOfReducers() {
    return this.getReducers().size();
  }
  
  public int getAmountOfUnassignedMappers() {
    int numAssignedMappers = 0;
    for(MapperWorkerInfo info : getMappers()) {
      if(info.getState() == WorkerState.WORKER_NOT_ASSIGNED) {
        ++numAssignedMappers;
      }
    }
    return numAssignedMappers;
  }
  
  public int getAmountOfUnassignedReducers() {
    int numAssignedReducers = 0;
    for(ReducerWorkerInfo info : getReducers()) {
      if(info.getState() == WorkerState.WORKER_NOT_ASSIGNED) {
        ++numAssignedReducers;
      }
    }
    return numAssignedReducers;
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
