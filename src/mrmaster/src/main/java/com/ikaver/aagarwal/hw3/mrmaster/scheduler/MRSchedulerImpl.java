package com.ikaver.aagarwal.hw3.mrmaster.scheduler;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;

import org.apache.log4j.Logger;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.ikaver.aagarwal.hw3.common.config.MRConfig;
import com.ikaver.aagarwal.hw3.common.definitions.Definitions;
import com.ikaver.aagarwal.hw3.common.dfs.FileMetadata;
import com.ikaver.aagarwal.hw3.common.dfs.IDFS;
import com.ikaver.aagarwal.hw3.common.nodemanager.IMRNodeManager;
import com.ikaver.aagarwal.hw3.common.nodemanager.NodeManagerFactory;
import com.ikaver.aagarwal.hw3.common.util.SocketAddress;
import com.ikaver.aagarwal.hw3.common.workers.MapWorkDescription;
import com.ikaver.aagarwal.hw3.common.workers.ReduceWorkDescription;
import com.ikaver.aagarwal.hw3.common.workers.WorkerState;

/**
 * Scheduler implementation.  Listens for requests of the MR Master to schedule
 *  mapper or reducer jobs. Whenever a new request comes, the scheduler selects 
 *  the node manager more appropriate to run the job and sends over the job 
 *  request to him. The scheduler tries to assign the job to node managers 
 *  that already have the data locally, and that don't have too many jobs 
 *  running already.
 */
@Singleton
public class MRSchedulerImpl implements IMRScheduler {

  private IDFS dfs;
  private Map<SocketAddress, NodeInformation> nodeInfo;
  private Set<SocketAddress> allNodes;
  private ReadWriteLock nodeInfoLock;
  private ScheduledExecutorService scheduler;

  private static final Logger LOG = Logger.getLogger(MRSchedulerImpl.class);

  @Inject
  public MRSchedulerImpl(
      @Named(Definitions.SCHEDULER_NODES_INFORMATION_MAP)
      Map<SocketAddress, NodeInformation> availableNodes,
      @Named(Definitions.SCHEDULER_NODES_INFORMATION_LOCK)ReadWriteLock availableNodesLock,
      @Named(Definitions.NODE_MANAGER_SET_ANNOTATION) Set<SocketAddress> allNodes,
      IDFS dfs) {
    this.nodeInfo = availableNodes;
    this.nodeInfoLock = availableNodesLock;
    this.dfs = dfs;
    this.allNodes = allNodes;

    //Start node tracker service (keeps track of node performance).
    this.scheduler = Executors.newScheduledThreadPool(1);
    NodeTracker tracker = new NodeTracker(this.nodeInfo, this.allNodes, this.nodeInfoLock);
    scheduler.scheduleAtFixedRate(tracker, 0,
        MRConfig.getTimeToCheckForNodeManagerState(), TimeUnit.SECONDS);
  }

  public Set<MapperWorkerInfo> runMappersForWork(Set<MapWorkDescription> work) {
    Set<MapperWorkerInfo> info = new HashSet<MapperWorkerInfo>();
    for(MapWorkDescription workToDo : work) {
      NodeManagerWithSocketAddress worker = null;
      //try to find a worker
      LOG.info("Trying to find node manager for mapper");
      worker = nodeManagerForMapperWork(workToDo);
      LOG.info("Got node manager: " + worker);
      if(worker != null) {
        try {
          LOG.info("Will ask mapper to start work: " + worker.sa);
          boolean success = worker.nm.doMap(workToDo);
          if(success) {
            LOG.info("Mapper started working: " + worker.sa);
            MapperWorkerInfo workerInfo = new MapperWorkerInfo(
                workToDo,
                worker.sa,
                WorkerState.RUNNING
                );
            this.nodeInfoLock.writeLock().lock();
            NodeInformation nodeInfo = this.nodeInfo.get(worker.sa);
            if(nodeInfo != null) nodeInfo.setAvailableSlots(nodeInfo.getAvailableSlots()-1);
            this.nodeInfoLock.writeLock().unlock();
            info.add(workerInfo);
          }
          else {
            LOG.info("Mapper return false, failed to start working...");
            info.add(mapperWorkerInfoForFailure(workToDo));
          } 
        } catch (RemoteException e) {
          LOG.warn("Failed to launch mapper", e);
          info.add(mapperWorkerInfoForFailure(workToDo));
        }
      }
      else {
        info.add(new MapperWorkerInfo(
            workToDo, null, WorkerState.WORKER_NOT_ASSIGNED
            ));
      }

    }
    return info;
  }

  private MapperWorkerInfo mapperWorkerInfoForFailure(MapWorkDescription workToDo) {
    LOG.info("Failed to launch mapper. Creating failed worker for now");
    MapperWorkerInfo workerInfo = new MapperWorkerInfo(
        workToDo,
        null,
        WorkerState.FAILED
        );
    return workerInfo;
  }

  public Set<ReducerWorkerInfo> runReducersForWork(
      Set<ReduceWorkDescription> work) {
    Set<ReducerWorkerInfo> info = new HashSet<ReducerWorkerInfo>();
    for(ReduceWorkDescription workToDo : work) {
      NodeManagerWithSocketAddress worker = nodeManagerForReducer();
      if(worker != null) {
        try {
          LOG.info("Will ask reducer to start work: " + worker.sa);
          boolean success = worker.nm.doReduce(workToDo);
          if(success){
            LOG.info("Reducer started working: " + worker.sa);
            ReducerWorkerInfo workerInfo = new ReducerWorkerInfo(
                workToDo,
                worker.sa,
                WorkerState.RUNNING
                );
            this.nodeInfoLock.writeLock().lock();
            NodeInformation nodeInfo = this.nodeInfo.get(worker.sa);
            if(nodeInfo != null) nodeInfo.setAvailableSlots(nodeInfo.getAvailableSlots()-1);
            this.nodeInfoLock.writeLock().unlock();
            info.add(workerInfo);
          }
          else {
            LOG.warn("Failed to launch reducer");
            ReducerWorkerInfo workerInfo = new ReducerWorkerInfo(
                workToDo,
                null,
                WorkerState.FAILED
                );
            info.add(workerInfo);
          }
        } catch (RemoteException e) {
          LOG.warn("Failed to launch reducer", e);
          ReducerWorkerInfo workerInfo = new ReducerWorkerInfo(
              workToDo,
              null,
              WorkerState.FAILED
              );
          info.add(workerInfo);
        }
      }
      else {
        ReducerWorkerInfo workerInfo = new ReducerWorkerInfo(
            workToDo,
            null,
            WorkerState.WORKER_NOT_ASSIGNED
            );
        info.add(workerInfo);
      }
    }
    return info;
  }

  /**
   * Returns the recommended node manager for the given map work description.
   * @param work
   * @return the node manager that has higher availability to run the job.
   * The return value may be null.
   */
  private NodeManagerWithSocketAddress nodeManagerForMapperWork(MapWorkDescription work) {
    FileMetadata metadata = null;
    try {
      metadata = dfs.getMetadata(work.getChunk().getInputFilePath());
    } catch (RemoteException e) {
      LOG.warn("Failed to communicate with DFS", e);
    }
    if(metadata == null) {
      LOG.warn("Couldn't find metadata for work: " + work);
      return null;
    }
    else{
      LOG.info("Searching for mappers with data for work: " + work);
      //get nodes that actually have chunk locally
      Set<SocketAddress> nodeManagersForChunk 
      = metadata.getNumChunkToAddr().get(work.getChunk().getPartitionID());

      for(SocketAddress addr : nodeManagersForChunk) {
        LOG.info("NM has the data: " + addr);
      }

      this.nodeInfoLock.readLock().lock();
      //get intersection of available nodes and nodes that have chunk.
      nodeManagersForChunk.retainAll(this.nodeInfo.keySet());
      //select the best node manager
      SocketAddress selectedNode = selectNodeFromSet(nodeManagersForChunk);
      LOG.info("Selected node for work: " + work + " is: " + selectedNode);
      this.nodeInfoLock.readLock().unlock();

      IMRNodeManager nm = NodeManagerFactory.nodeManagerFromSocketAddress(selectedNode);
      if(nm != null)
        return new NodeManagerWithSocketAddress(nm, selectedNode);
      else return null;
    }
  }

  /**
   * Selects the node most appropriate for a new map task from the given set of 
   * nodes. Assumes that the read lock is acquired for the nodeinfo ds.
   * @param nodes
   * @return
   */
  private SocketAddress selectNodeFromSet(Set<SocketAddress> nodes) {
    SocketAddress selectedAddr = null;
    double highestScore = -1;
    for(SocketAddress node : nodes) {
      NodeInformation info = this.nodeInfo.get(node);
      if(info != null && info.getAvailableSlots() > 0) {
        double scoreForNode = scoreForNode(info);
        if(selectedAddr == null || scoreForNode > highestScore) {
          highestScore = scoreForNode;
          selectedAddr = node;
        }
      }
    }
    return selectedAddr;
  }

  /**
   * Returns the score of the node with the given node information
   * @param nodeInfo
   * @return the score of the node, the higher the score, the more likely
   * it is that we will choose this node for the job.
   */
  private double scoreForNode(NodeInformation nodeInfo) {
    if (nodeInfo == null) return -1;
    else return nodeInfo.getNumCores() * nodeInfo.getAvailableSlots();
  }

  /**
   * Finds a node manager for a reducer task. The node manager is selected
   * randomly for now.
   * @return
   */
  private NodeManagerWithSocketAddress nodeManagerForReducer() {
    this.nodeInfoLock.readLock().lock();
    List<SocketAddress> nmList = new ArrayList<SocketAddress>(this.nodeInfo.keySet());
    this.nodeInfoLock.readLock().unlock();
    Collections.shuffle(nmList);

    this.nodeInfoLock.readLock().lock();
    SocketAddress randomAddr = null;
    for(int i = 0; i < nmList.size(); ++i) {
      NodeInformation info = this.nodeInfo.get(nmList.get(i));
      if(info.getAvailableSlots() > 0) {
        randomAddr = nmList.get(i);
        break;
      }
    }
    this.nodeInfoLock.readLock().unlock();
    IMRNodeManager availableNodeManager = NodeManagerFactory.nodeManagerFromSocketAddress(randomAddr);
    if(availableNodeManager != null)
      return new NodeManagerWithSocketAddress(availableNodeManager, randomAddr);
    else return null;
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
