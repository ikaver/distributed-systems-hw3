package com.ikaver.aagarwal.hw3.mrmaster.jobmanager;

import java.util.HashSet;
import java.util.Set;

import com.ikaver.aagarwal.hw3.mrmaster.scheduler.MapperWorkerInfo;
import com.ikaver.aagarwal.hw3.mrmaster.scheduler.ReducerWorkerInfo;

public class RunningJob {
  
  private int jobID;
  private Set<MapperWorkerInfo> mappers;
  private Set<ReducerWorkerInfo> reducers;

  public RunningJob(int jobID) {
    this.jobID = jobID;
    this.mappers = new HashSet<MapperWorkerInfo>();
    this.reducers = new HashSet<ReducerWorkerInfo>();
  }

  public int getJobID() {
    return jobID;
  }

  public Set<MapperWorkerInfo> getMappers() {
    return mappers;
  }

  public Set<ReducerWorkerInfo> getReducers() {
    return reducers;
  }
  
}
