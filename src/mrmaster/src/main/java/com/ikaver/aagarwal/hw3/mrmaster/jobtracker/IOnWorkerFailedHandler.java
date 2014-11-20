package com.ikaver.aagarwal.hw3.mrmaster.jobtracker;

import com.ikaver.aagarwal.hw3.mrmaster.jobmanager.RunningJob;
import com.ikaver.aagarwal.hw3.mrmaster.scheduler.MapperWorkerInfo;
import com.ikaver.aagarwal.hw3.mrmaster.scheduler.ReducerWorkerInfo;

public interface IOnWorkerFailedHandler {

  public void onMapperFailed(RunningJob job, MapperWorkerInfo info);
  public void onReducerFailed(RunningJob job, ReducerWorkerInfo info);
  public void onMapperNotAssignedFound(RunningJob job, MapperWorkerInfo info);
  public void onReducerNotAssignedFound(RunningJob job, ReducerWorkerInfo info);
  
}
