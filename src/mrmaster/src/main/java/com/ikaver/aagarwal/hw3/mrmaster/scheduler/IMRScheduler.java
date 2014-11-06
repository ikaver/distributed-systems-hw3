package com.ikaver.aagarwal.hw3.mrmaster.scheduler;

import java.util.Set;

import com.ikaver.aagarwal.hw3.common.workers.MapWorkDescription;
import com.ikaver.aagarwal.hw3.common.workers.ReduceWorkDescription;

public interface IMRScheduler {
  
  public Set<MapperWorkerInfo> runMappersForWork(Set<MapWorkDescription> work);
  public Set<ReducerWorkerInfo> runReducersForWork(Set<ReduceWorkDescription> work);
  
}
