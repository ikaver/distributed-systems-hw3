package com.ikaver.aagarwal.hw3.mrmaster.scheduler;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import com.google.inject.name.Named;
import com.ikaver.aagarwal.hw3.common.definitions.Definitions;
import com.ikaver.aagarwal.hw3.common.nodemanager.IMRNodeManager;
import com.ikaver.aagarwal.hw3.common.util.SocketAddress;
import com.ikaver.aagarwal.hw3.common.workers.MapWorkDescription;
import com.ikaver.aagarwal.hw3.common.workers.ReduceWorkDescription;
import com.ikaver.aagarwal.hw3.common.workers.WorkerState;

public class IMRSchedulerImpl implements IMRScheduler {
  
  private Set<SocketAddress> workerNodes;
  private int currentReducerID;
  
  private static final int AMOUNT_RETRIES = 5;
  private static final Logger LOG = Logger.getLogger(IMRSchedulerImpl.class);
  
  public IMRSchedulerImpl(
      @Named(Definitions.NODE_MANAGER_SET_ANNOTATION)Set<SocketAddress> nodes) {
    this.workerNodes = nodes;
  }

  public Set<MapperWorkerInfo> runMappersForWork(Set<MapWorkDescription> work) {
    Set<MapperWorkerInfo> info = new HashSet<MapperWorkerInfo>();
    for(MapWorkDescription workToDo : work) {
      NodeManagerWithSocketAddress worker = null;
      for(int i = 0; i < AMOUNT_RETRIES && worker == null; ++i) {
        worker = availableNodeManager();
      }
      if(worker != null) {
        try {
          worker.nm.doMap(workToDo);
          MapperWorkerInfo workerInfo = new MapperWorkerInfo(
              workToDo.getJobID(),
              worker.sa,
              WorkerState.RUNNING,
              workToDo.getChunk(),
              workToDo.getJarPath(),
              workToDo.getMapperClass()
          );
          info.add(workerInfo);
        } catch (RemoteException e) {
          LOG.warn("Failed to launch mapper", e);
          MapperWorkerInfo workerInfo = new MapperWorkerInfo(
              workToDo.getJobID(),
              null,
              WorkerState.WORKER_NOT_ASSIGNED,
              workToDo.getChunk(),
              workToDo.getJarPath(),
              workToDo.getMapperClass()
          );
          info.add(workerInfo);
        }
      }
    }
    return info;
  }

  public Set<ReducerWorkerInfo> runReducersForWork(
      Set<ReduceWorkDescription> work) {
    Set<ReducerWorkerInfo> info = new HashSet<ReducerWorkerInfo>();
    for(ReduceWorkDescription workToDo : work) {
      NodeManagerWithSocketAddress worker = null;
      for(int i = 0; i < AMOUNT_RETRIES && worker == null; ++i) {
        worker = availableNodeManager();
      }
      if(worker != null) {
        try {
          worker.nm.doReduce(workToDo);
          ReducerWorkerInfo workerInfo = new ReducerWorkerInfo(
              workToDo.getJobID(),
              worker.sa,
              WorkerState.RUNNING,
              getNewReducerId(),
              workToDo.getInputSources(),
              workToDo.getMapperChunks(),
              workToDo.getOutputFilePath()
          );
          info.add(workerInfo);
        } catch (RemoteException e) {
          LOG.warn("Failed to launch reducer", e);
          ReducerWorkerInfo workerInfo = new ReducerWorkerInfo(
              workToDo.getJobID(),
              null,
              WorkerState.WORKER_NOT_ASSIGNED,
              getNewReducerId(),
              workToDo.getInputSources(),
              workToDo.getMapperChunks(),
              workToDo.getOutputFilePath()
          );
          info.add(workerInfo);
        }
      }
    }
    return info;
  }
 
  private NodeManagerWithSocketAddress availableNodeManager() {
    List<SocketAddress> dataNodesList = new ArrayList<SocketAddress>(this.workerNodes);
    Collections.shuffle(dataNodesList);
    SocketAddress randomAddr = dataNodesList.get(0);
    IMRNodeManager availableNodeManager = NodeManagerFactory.nodeManagerFromSocketAddress(randomAddr);
    if(availableNodeManager != null)
      return new NodeManagerWithSocketAddress(availableNodeManager, randomAddr);
    else return null;
  }
  
  private int getNewReducerId() {
    return ++this.currentReducerID;
  }

  private class NodeManagerWithSocketAddress {
    public final IMRNodeManager nm;
    public final SocketAddress sa;
    
    public NodeManagerWithSocketAddress(IMRNodeManager nm, SocketAddress sa) {
      this.nm = nm;
      this.sa = sa;
    }
  }
  
}
