package com.ikaver.aagarwal.hw3.common.definitions;

public class Definitions {

  //services
  public static final String JOB_MANAGER_SERVICE = "JobManager";
  public static final String MR_NODE_MANAGER_SERVICE = "NodeManager";
  public static final String DFS_SERVICE = "DFS";
  public static final String DATA_NODE_SERVICE = "DataNode";
  public static final String MR_MAP_RUNNER_SERVICE = "MrMapRunner";
  public static final String MR_REDUCE_RUNNER_SERVICE = "MrReduceRunner";
  
  //annotations
  public static final String MASTER_SOCKET_ADDR_ANNOTATION = "MasterSocketAddr";
  public static final String NODE_MANAGER_SET_ANNOTATION = "NodeManagerSet";
  
  public static final String SCHEDULER_NODES_INFORMATION_MAP = "SchedulerNodeInfoMaps";
  public static final String SCHEDULER_NODES_INFORMATION_LOCK = "SchedulerWorkerNodes";
  
  public static final String DFS_REPLICATION_FACTOR_ANNOTATION = "DFSReplicationFactor";
  public static final String DFS_MAP_FILE_TO_METADATA_ANNOTATION = "DFSMapFileToMetadata";
  public static final String DFS_DATA_NODES_ANNOTATION = "DFSDataNodes";
  public static final String DFS_DATA_NODES_SET_LOCK_ANNOTATION = "DFSDataNodesLock";
  public static final String DFS_MAP_FILE_TO_METADATA_LOCK_ANNOTATION = "DFSMapFileToMetadataLock";
  
  //jobmanager
  public static final int MAX_WORKER_RETRIES_BEFORE_CANCELLING_JOB = 5;
  
  //Scheduler
  public static final int SCHEDULER_TIME_TO_CHECK_FOR_NODES_STATE = 3;
  
  //JobTracker
  public static final int TIME_TO_CHECK_FOR_NODE_MANAGER_STATE = 3;
  
  //DFS
  public static final String BASE_DIRECTORY = "/tmp/mrikav-ank-dfs/";
  // Local File system base directory
  public static final String LOCAL_FS_BASE_DIRECTORY = "/tmp/mrikav-ank-local";
  public static final int REPLICATION_FACTOR = 2;
  public static final int SIZE_OF_CHUNK = (1 << 19); //512KB
  public static final int NUM_DFS_READ_RETRIES = 3;

  public static final int NODE_MANAGER_SERVICE_PORT = 3000;
}
