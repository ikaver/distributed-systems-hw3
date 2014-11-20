package com.ikaver.aagarwal.hw3.mrmaster.main;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import com.ikaver.aagarwal.hw3.common.definitions.Definitions;
import com.ikaver.aagarwal.hw3.common.dfs.FileMetadata;
import com.ikaver.aagarwal.hw3.common.dfs.IDFS;
import com.ikaver.aagarwal.hw3.common.master.IJobManager;
import com.ikaver.aagarwal.hw3.common.util.SocketAddress;
import com.ikaver.aagarwal.hw3.mrdfs.master.DFSImpl;
import com.ikaver.aagarwal.hw3.mrmaster.jobmanager.JobManagerImpl;
import com.ikaver.aagarwal.hw3.mrmaster.jobmanager.JobsState;
import com.ikaver.aagarwal.hw3.mrmaster.jobmanager.jobvalidator.IJobValidator;
import com.ikaver.aagarwal.hw3.mrmaster.jobmanager.jobvalidator.JobValidatorImpl;
import com.ikaver.aagarwal.hw3.mrmaster.scheduler.IMRScheduler;
import com.ikaver.aagarwal.hw3.mrmaster.scheduler.MRSchedulerImpl;
import com.ikaver.aagarwal.hw3.mrmaster.scheduler.NodeInformation;

public class MRMasterModule extends AbstractModule {

  private Set<SocketAddress> dataNodes;
  private Set<SocketAddress> nodes;
  
  public MRMasterModule(Set<SocketAddress> nodes) {
    this.nodes = nodes;
    this.dataNodes = new HashSet<SocketAddress>(nodes);
  }
  
  @Override
  protected void configure() {
    bind(IJobManager.class).to(JobManagerImpl.class);
    bind(IDFS.class).to(DFSImpl.class);
    bind(IMRScheduler.class).to(MRSchedulerImpl.class);
    bind(IJobValidator.class).to(JobValidatorImpl.class);
    
    //Master DFS setup
    ReadWriteLock dfsDataNodesLock = new ReentrantReadWriteLock();
    ReadWriteLock dfsMapLock = new ReentrantReadWriteLock();
    HashMap<String, FileMetadata> dfsMap = new HashMap<String, FileMetadata>();
    bind(ReadWriteLock.class)
      .annotatedWith(Names.named(Definitions.DFS_DATA_NODES_SET_LOCK_ANNOTATION))
      .toInstance(dfsDataNodesLock);
    bind(ReadWriteLock.class)
    .annotatedWith(Names.named(Definitions.DFS_MAP_FILE_TO_METADATA_LOCK_ANNOTATION))
    .toInstance(dfsMapLock);
    bind(new TypeLiteral<Map<String, FileMetadata>>(){})
      .annotatedWith(Names.named(Definitions.DFS_MAP_FILE_TO_METADATA_ANNOTATION))
      .toInstance(dfsMap);
    bind(new TypeLiteral<Set<SocketAddress>>(){})
      .annotatedWith(Names.named(Definitions.DFS_DATA_NODES_ANNOTATION))
      .toInstance(dataNodes);
    bind(Integer.class)
      .annotatedWith(Names.named(Definitions.DFS_REPLICATION_FACTOR_ANNOTATION))
      .toInstance(Definitions.REPLICATION_FACTOR);

    
    //Job manager setup
    JobsState jobsState = new JobsState();
    bind(JobsState.class).toInstance(jobsState);
    bind(new TypeLiteral<Set<SocketAddress>>(){})
    .annotatedWith(Names.named(Definitions.NODE_MANAGER_SET_ANNOTATION))
    .toInstance(nodes);   

    
    //Scheduler setup
    Map<SocketAddress, NodeInformation> nodeInfo = new HashMap<SocketAddress, NodeInformation>();
    
    ReadWriteLock schedulerWorkerLock = new ReentrantReadWriteLock();
    bind(new TypeLiteral<Map<SocketAddress, NodeInformation>>(){})
      .annotatedWith(Names.named(Definitions.SCHEDULER_NODES_INFORMATION_MAP))
      .toInstance(nodeInfo);   
    bind(ReadWriteLock.class)
      .annotatedWith(Names.named(Definitions.SCHEDULER_NODES_INFORMATION_LOCK))
      .toInstance(schedulerWorkerLock);
    
  }

}
