package com.ikaver.aagarwal.hw3.mrmaster.jobtracker;

import java.rmi.RemoteException;
import java.security.InvalidParameterException;

import com.ikaver.aagarwal.hw3.common.nodemanager.IMRNodeManager;
import com.ikaver.aagarwal.hw3.common.workers.WorkerState;
import com.ikaver.aagarwal.hw3.mrmaster.jobmanager.RunningJob;
import com.ikaver.aagarwal.hw3.mrmaster.scheduler.MapperWorkerInfo;
import com.ikaver.aagarwal.hw3.mrmaster.scheduler.NodeManagerFactory;
import com.ikaver.aagarwal.hw3.mrmaster.scheduler.ReducerWorkerInfo;

public class JobTracker implements Runnable {

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
        this.onWorkerFailedHandler.onMapperFailed(job, info);
      }
      else if(info.getState() == WorkerState.RUNNING) {
        //query the mapper for its state
        IMRNodeManager nm = NodeManagerFactory.nodeManagerFromSocketAddress(info.getNodeManagerAddress());
        if(nm == null) {
          info.setState(WorkerState.FAILED);
        }
        else {
          try {
            info.setState(nm.getMapperState(info.getJobID(), info.getChunk().getPartitionID()));
          } catch (RemoteException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
        }
      }
      else if(info.getState() == WorkerState.FAILED) {
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
    }
  }

  public void queryReducers() {
    for(ReducerWorkerInfo info : job.getReducers()) {
      if(info.getState() == WorkerState.WORKER_NOT_ASSIGNED) {
        this.onWorkerFailedHandler.onReducerFailed(job, info);
      }
      else if(info.getState() == WorkerState.RUNNING) {
        IMRNodeManager nm = NodeManagerFactory.nodeManagerFromSocketAddress(info.getNodeManagerAddress());
        if(nm == null) {
          info.setState(WorkerState.FAILED);
        }
        else {
          try {
            info.setState(nm.getReducerState(info.getJobID(), info.getReducerID()));
          } catch (RemoteException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
        }
      }
      else if(info.getState() == WorkerState.FAILED) {
        this.onWorkerFailedHandler.onReducerFailed(job, info);
      }
      else if(info.getState() == WorkerState.FINISHED
          && !job.getFinishedReducers().contains(info)) {
        this.onWorkCompletedHandler.onReducerFinished(job, info);
      }
    }
    if(job.getAmountOfReducers() == job.getAmountOfFinishedReducers()
        && !notifiedReducersCompleted) {
      this.onWorkCompletedHandler.onAllReducersFinished(job);
    }
  }

}


