package com.ikaver.aagarwal.hw3.common.definitions;

public class Definitions {

  //services
  public static final String JOB_MANAGER_SERVICE = "JobManager";
  public static final String MR_NODE_MANAGER_SERVICE = "NodeManager";
  public static final String DFS_SERVICE = "DFS";
  public static final String DATA_NODE_SERVICE = "DataNode";
  public static final String MR_MAP_RUNNER_SERVICE = "MrMapRunner";
  
  //annotations
  public static final String MASTER_SOCKET_ADDR_ANNOTATION = "MasterSocketAddr";
  public static final String NODE_MANAGER_SET_ANNOTATION = "NodeManagerSet";
  
  public static final String DFS_REPLICATION_FACTOR_ANNOTATION = "DFSReplicationFactor";
  public static final String DFS_MAP_PATH_TO_FILE_ANNOTATION = "DFSPathToFile";
  public static final String DFS_DATA_NODES_ANNOTATION = "DFSDataNodes";
  public static final String DFS_MAP_LOCK_ANNOTATION = "DFSMapLock";
  
  //JobTracker
  public static final int TIME_TO_CHECK_FOR_NODE_MANAGER_STATE = 3;
  
  //DFS
  public static final String BASE_DIRECTORY = "/tmp/mrikav-ank-dfs/";
  public static final int REPLICATION_FACTOR = 3;

}
