package com.ikaver.aagarwal.hw3.mrmaster.jobtracker;

import com.ikaver.aagarwal.hw3.mrmaster.jobmanager.RunningJob;
import com.ikaver.aagarwal.hw3.mrmaster.scheduler.MapperWorkerInfo;
import com.ikaver.aagarwal.hw3.mrmaster.scheduler.ReducerWorkerInfo;

public interface IOnWorkCompletedHandler {
  
  public void onMapperFinished(RunningJob job, MapperWorkerInfo info);
  public void onReducerFinished(RunningJob job, ReducerWorkerInfo info);
  public void onAllMappersFinished(RunningJob job);
  public void onAllReducersFinished(RunningJob job);

}
