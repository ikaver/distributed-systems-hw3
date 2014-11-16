package com.ikaver.aagarwal.hw3.mrnodemanager.workertracker;

import java.rmi.RemoteException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;

import org.apache.log4j.Logger;

import com.ikaver.aagarwal.hw3.common.mrmap.IMapInstanceRunner;
import com.ikaver.aagarwal.hw3.common.util.SocketAddress;
import com.ikaver.aagarwal.hw3.common.workers.MapWorkDescription;
import com.ikaver.aagarwal.hw3.common.workers.WorkerState;
import com.ikaver.aagarwal.hw3.mrnodemanager.util.MapInstanceRunnerFactory;

public class WorkerTracker implements Runnable {
  
  private static final Logger LOG = Logger.getLogger(WorkerTracker.class);
  
  private Map<MapWorkDescription, SocketAddress> workerAddressMap;
  private ReadWriteLock workerAddressMapLock;
  private IWorkerTrackerDelegate delegate;
  
  public void run() {
    queryMappers();
  }
  
  private void queryMappers() {
    workerAddressMapLock.readLock().lock();
    Set<MapWorkDescription> mappers = workerAddressMap.keySet();
    workerAddressMapLock.readLock().unlock();
    for(MapWorkDescription mapper : mappers) {
      IMapInstanceRunner runner 
      = MapInstanceRunnerFactory.mapRunnerFromSocketAddress(workerAddressMap.get(mapper));
      if(runner != null) {
        try {
          if(runner.getMapperState() == WorkerState.FINISHED) {
            delegate.onMapperFinished(mapper, runner); 
            workerAddressMap.remove(mapper);
          }
          else if(runner.getMapperState() == WorkerState.FAILED) {
            delegate.onMapperFailed(mapper);
            workerAddressMap.remove(mapper);
          }
        } catch (RemoteException e) {
          LOG.warn("Map instance runner remote exception", e);
          delegate.onMapperFailed(mapper);
          workerAddressMap.remove(mapper);
        }
      }
    }
  }
  
  
  

}
