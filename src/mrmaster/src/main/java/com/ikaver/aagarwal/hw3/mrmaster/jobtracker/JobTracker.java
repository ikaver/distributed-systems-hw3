package com.ikaver.aagarwal.hw3.mrmaster.jobtracker;

import java.rmi.RemoteException;
import java.security.InvalidParameterException;

import org.apache.log4j.Logger;

import com.ikaver.aagarwal.hw3.common.nodemanager.IMRNodeManager;
import com.ikaver.aagarwal.hw3.common.nodemanager.NodeManagerFactory;
import com.ikaver.aagarwal.hw3.common.workers.WorkerState;
import com.ikaver.aagarwal.hw3.mrmaster.jobmanager.RunningJob;
import com.ikaver.aagarwal.hw3.mrmaster.scheduler.MapperWorkerInfo;
import com.ikaver.aagarwal.hw3.mrmaster.scheduler.ReducerWorkerInfo;

public class JobTracker implements Runnable {

  private static final Logger LOG = Logger.getLogger(JobTracker.class);

  private RunningJob job;
  private IOnWorkerFailedHandler onWorkerFailedHandler;
  private IOnWorkCompletedHandler onWorkCompletedHandler;
  private boolean notifiedMappersCompleted;
  private boolean notifiedReducersCompleted;

  public JobTracker(RunningJob job, IOnWorkerFailedHandler onWorkerFailed,
      IOnWorkCompletedHandler onWorkCompleted) {
    if(job == null) throw new InvalidParameterException("Job cannot be null");
    if(onWorkerFailed == null) throw new InvalidParameterException("On worker failed handler cannot be null");
    if(onWorkCompleted == null) throw new InvalidParameterException("On work completed handler cannot be null");
    this.job = job;
    this.onWorkerFailedHandler = onWorkerFailed;
    this.onWorkCompletedHandler = onWorkCompleted;
    this.notifiedMappersCompleted = false;
    this.notifiedReducersCompleted = false;
  }

  public void run() {
    this.queryMappers();
    this.queryReducers();
  }

  public void queryMappers() {
    for(MapperWorkerInfo info : job.getMappers()) {
      if(info.getState() == WorkerState.WORKER_NOT_ASSIGNED) {
        //ask scheduler to assign a worker
        this.onWorkerFailedHandler.onMapperNotAssignedFound(job, info);
      }
      else if(info.getState() == WorkerState.RUNNING) {
        //query the mapper for its state
        IMRNodeManager nm = NodeManagerFactory.nodeManagerFromSocketAddress(info.getNodeManagerAddress());
        if(nm == null) {
          info.setState(WorkerState.FAILED);
        }
        else {
          try {
            WorkerState state = nm.getMapperState(info.getWorkDescription());
            info.setState(state);
            if(info.getState() == null) info.setState(WorkerState.FAILED);
            if(info.getState() == null || info.getState() == WorkerState.FAILED) {
              LOG.info("Got state " + state + " from mapper " 
                  + info.getNodeManagerAddress() + " " 
                  + info.getWorkDescription().getChunk().getPartitionID());
            }
          } catch (RemoteException e) {
            info.setState(WorkerState.FAILED);
            LOG.warn("Failed to get nm state", e);
          }
        }
      }
      else if(info.getState() == WorkerState.FAILED
          || info.getState() == WorkerState.WORKER_DOESNT_EXIST
          || info.getState() == null) {
        info.setState(WorkerState.FAILED);
        //ask scheduler to create a new mapper
        this.onWorkerFailedHandler.onMapperFailed(job, info);
      }
      else if(info.getState() == WorkerState.FINISHED
          && !job.getFinishedMappers().contains(info)) {
        this.onWorkCompletedHandler.onMapperFinished(job, info);
      }
    }
    if(job.getAmountOfMappers() == job.getAmountOfFinishedMappers()
        && !notifiedMappersCompleted) {
      this.onWorkCompletedHandler.onAllMappersFinished(job);
      this.notifiedMappersCompleted = true;
    }
  }

  public void queryReducers() {
    for(ReducerWorkerInfo info : job.getReducers()) {
      if(info.getState() == WorkerState.WORKER_NOT_ASSIGNED) {
        this.onWorkerFailedHandler.onReducerNotAssignedFound(job, info);
      }
      else if(info.getState() == WorkerState.RUNNING) {
        IMRNodeManager nm = NodeManagerFactory.nodeManagerFromSocketAddress(info.getNodeManagerAddress());
        if(nm == null) {
          info.setState(WorkerState.FAILED);
        }
        else {
          try {
            WorkerState state = nm.getReducerState(info.getWorkDescription());
            info.setState(state);
            if(info.getState() == null) info.setState(WorkerState.FAILED);
            if(state == null || state == WorkerState.FAILED) {
              LOG.info("Got state " + state + " from reducer " 
                  + info.getNodeManagerAddress() + " " 
                  + info.getWorkDescription().getReducerID());
            }
          } catch (RemoteException e) {
            info.setState(WorkerState.FAILED);
            LOG.warn("Failed to get nm state", e);
          }
        }
      }
      else if(info.getState() == WorkerState.FAILED 
          || info.getState() == WorkerState.WORKER_DOESNT_EXIST  
          || info.getState() == null) {
        info.setState(WorkerState.FAILED);
        this.onWorkerFailedHandler.onReducerFailed(job, info);
      }
      else if(info.getState() == WorkerState.FINISHED
          && !job.getFinishedReducers().contains(info)) {
        this.onWorkCompletedHandler.onReducerFinished(job, info);
      }
    }
    if(job.getAmountOfReducers() == job.getAmountOfFinishedReducers()
        && !notifiedReducersCompleted && job.getMappersFinished()) {
      this.onWorkCompletedHandler.onAllReducersFinished(job);
      this.notifiedReducersCompleted = true;
    }
  }

}


