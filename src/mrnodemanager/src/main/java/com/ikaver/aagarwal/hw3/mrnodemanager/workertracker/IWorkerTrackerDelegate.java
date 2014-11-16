package com.ikaver.aagarwal.hw3.mrnodemanager.workertracker;

import com.ikaver.aagarwal.hw3.common.mrmap.IMapInstanceRunner;
import com.ikaver.aagarwal.hw3.common.workers.MapWorkDescription;

public interface IWorkerTrackerDelegate {
  
  public void onMapperFailed(MapWorkDescription mapper);
  public void onMapperFinished(MapWorkDescription mapper, IMapInstanceRunner runner);

}
