package com.ikaver.aagarwal.hw3.mrmaster.jobtracker;

import com.ikaver.aagarwal.hw3.mrmaster.scheduler.MapperWorkerInfo;
import com.ikaver.aagarwal.hw3.mrmaster.scheduler.ReducerWorkerInfo;

public interface IOnWorkerFailedHandler {

  public void onMapperFailed(MapperWorkerInfo info);
  public void onReducerFailed(ReducerWorkerInfo info);
  
}
