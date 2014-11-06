package com.ikaver.aagarwal.hw3.mrmaster.jobmanager;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.ikaver.aagarwal.hw3.mrmaster.scheduler.MapperWorkerInfo;
import com.ikaver.aagarwal.hw3.mrmaster.scheduler.ReducerWorkerInfo;

public class JobsState {

  private Map<Integer, Set<MapperWorkerInfo>> jobIDToMappers;
  private Map<Integer, Set<ReducerWorkerInfo>> jobIDToReducers;
  
  public Set<MapperWorkerInfo> getMappersOfJob(int jobID) {
    return jobIDToMappers.get(jobID);
  }
  
  public Set<ReducerWorkerInfo> getReducersOfJob(int jobID) {
    return jobIDToReducers.get(jobID);
  }
  
  public void addMapperToJob(int jobID, MapperWorkerInfo info) {
    if(!jobIDToMappers.containsKey(jobID)) {
      jobIDToMappers.put(jobID, new HashSet<MapperWorkerInfo>());
    }
    Set<MapperWorkerInfo> workerInfo = jobIDToMappers.get(jobID);
    workerInfo.add(info);
  }
  
  public void addReducerToJob(int jobID, ReducerWorkerInfo info) {
    if(!jobIDToReducers.containsKey(jobID)) {
      jobIDToReducers.put(jobID, new HashSet<ReducerWorkerInfo>());
    }
    Set<ReducerWorkerInfo> workerInfo = jobIDToReducers.get(jobID);
    workerInfo.add(info);
  }

}
