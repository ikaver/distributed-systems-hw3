package com.ikaver.aagarwal.hw3.mrmaster.main;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import com.ikaver.aagarwal.hw3.common.definitions.Definitions;
import com.ikaver.aagarwal.hw3.common.dfs.IDFS;
import com.ikaver.aagarwal.hw3.common.master.IJobManager;
import com.ikaver.aagarwal.hw3.common.util.SocketAddress;
import com.ikaver.aagarwal.hw3.mrdfs.master.DFSImpl;
import com.ikaver.aagarwal.hw3.mrmaster.jobmanager.JobManagerMockImpl;
import com.ikaver.aagarwal.hw3.mrmaster.jobmanager.JobsState;
import com.ikaver.aagarwal.hw3.mrmaster.scheduler.IMRScheduler;
import com.ikaver.aagarwal.hw3.mrmaster.scheduler.IMRSchedulerImpl;

public class MRMasterModule extends AbstractModule {

  private Set<SocketAddress> nodes;
  
  public MRMasterModule(Set<SocketAddress> nodes) {
    this.nodes = nodes;
  }
  
  @Override
  protected void configure() {
    bind(IJobManager.class).to(JobManagerMockImpl.class);
    bind(IDFS.class).to(DFSImpl.class);
    bind(IMRScheduler.class).to(IMRSchedulerImpl.class);
    
    //Master DFS setup
    ReadWriteLock dfsLock = new ReentrantReadWriteLock();
    HashMap<String, Set<SocketAddress>> dfsMap = new HashMap<String, Set<SocketAddress>>();
    bind(ReadWriteLock.class)
      .annotatedWith(Names.named(Definitions.DFS_MAP_LOCK_ANNOTATION))
      .toInstance(dfsLock);
    bind(new TypeLiteral<Map<String, Set<SocketAddress>>>(){})
      .annotatedWith(Names.named(Definitions.DFS_MAP_PATH_TO_FILE_ANNOTATION))
      .toInstance(dfsMap);
    bind(new TypeLiteral<Set<SocketAddress>>(){})
      .annotatedWith(Names.named(Definitions.DFS_DATA_NODES_ANNOTATION))
      .toInstance(nodes);
    bind(Integer.class)
      .annotatedWith(Names.named(Definitions.DFS_REPLICATION_FACTOR_ANNOTATION))
      .toInstance(Definitions.REPLICATION_FACTOR);
    
    //Job manager setup
    JobsState jobsState = new JobsState();
    bind(JobsState.class).toInstance(jobsState);
    
    //Scheduler setup
    bind(new TypeLiteral<Set<SocketAddress>>(){})
      .annotatedWith(Names.named(Definitions.NODE_MANAGER_SET_ANNOTATION))
      .toInstance(nodes);   
    
  }

}
