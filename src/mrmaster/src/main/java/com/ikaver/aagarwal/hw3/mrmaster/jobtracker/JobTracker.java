package com.ikaver.aagarwal.hw3.mrmaster.jobtracker;

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

  public JobTracker(RunningJob job, IOnWorkerFailedHandler onWorkerFailed,
      IOnWorkCompletedHandler onWorkCompleted) {
    if(job == null) throw new InvalidParameterException("Job cannot be null");
    if(onWorkerFailed == null) throw new InvalidParameterException("On worker failed handler cannot be null");
    if(onWorkCompleted == null) throw new InvalidParameterException("On work completed handler cannot be null");
    this.job = job;
    this.onWorkerFailedHandler = onWorkerFailed;
    this.onWorkCompletedHandler = onWorkCompleted;
  }

  public void run() {
    this.queryMappers();
    this.queryReducers();
  }

  public void queryMappers() {
    if(job.getAmountOfMappers() == job.getAmountOfFinishedMappers()) {
      this.onWorkCompletedHandler.onAllMappersFinished(job);
    }
    else{
      for(MapperWorkerInfo info : job.getMappers()) {
        if(info.getState() == WorkerState.WORKER_NOT_ASSIGNED) {
          //ask scheduler to assign a worker
          this.onWorkerFailedHandler.onMapperFailed(info);
        }
        else if(info.getState() == WorkerState.RUNNING) {
          //query the mapper for its state
          IMRNodeManager nm = NodeManagerFactory.nodeManagerFromSocketAddress(info.getNodeManagerAddress());
          if(nm == null) {
            info.setState(WorkerState.FAILED);
          }
          else {
            info.setState(nm.getMapperState(info.getJobID(), info.getChunk().getPartitionID()));
          }
        }
        else if(info.getState() == WorkerState.FAILED) {
          //ask scheduler to create a new mapper
          this.onWorkerFailedHandler.onMapperFailed(info);
        }
        else if(info.getState() == WorkerState.FINISHED) {
          this.onWorkCompletedHandler.onMapperFinished(info);
        }
      }
    }
  }

  public void queryReducers() {
    if(job.getAmountOfReducers() == job.getAmountOfFinishedReducers()) {
      this.onWorkCompletedHandler.onAllReducersFinished(job);
    }
    else {
      for(ReducerWorkerInfo info : job.getReducers()) {
        if(info.getState() == WorkerState.WORKER_NOT_ASSIGNED) {
          this.onWorkerFailedHandler.onReducerFailed(info);
        }
        else if(info.getState() == WorkerState.RUNNING) {
          IMRNodeManager nm = NodeManagerFactory.nodeManagerFromSocketAddress(info.getNodeManagerAddress());
          if(nm == null) {
            info.setState(WorkerState.FAILED);
          }
          else {
            info.setState(nm.getReducerState(info.getJobID(), info.getReducerID()));
          }
        }
        else if(info.getState() == WorkerState.FAILED) {
          this.onWorkerFailedHandler.onReducerFailed(info);
        }
        else if(info.getState() == WorkerState.FINISHED) {
          this.onWorkCompletedHandler.onReducerFinished(info);
        }
      }
    }
  }

}


