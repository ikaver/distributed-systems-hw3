package com.ikaver.aagarwal.hw3.common.config;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Set;

import org.apache.log4j.Logger;

import com.ikaver.aagarwal.hw3.common.util.SocketAddress;

public class MRConfig {
  
  public static final Logger LOG = Logger.getLogger(MRConfig.class);
  
  //general
  private static SocketAddress masterSocketAddress;
  private static Set<SocketAddress> participantNodes;
  private static String configFileName;

  //job manager
  private static int maxWorkerRetriesBeforeCancellingJob;
  private static int timeToCheckForJobState;
  
  //scheduler
  private static int timeToCheckForNodeManagerState;
  
  //node manager
  private static int workersPerNode;
  
  //dfs
  private static int replicationFactor;
  private static int chunkSizeInBytes;
  private static int maxDFSReadRetries;
  private static int timeToCheckDataNodesState;

  public static SocketAddress getMasterSocketAddress() {
    return masterSocketAddress;
  }
  
  public static void setMasterSocketAddress(SocketAddress addr) {
    MRConfig.masterSocketAddress = addr;
  }
  
  public static Set<SocketAddress> getParticipantNodes() {
    return participantNodes;
  }

  public static void setParticipantNodes(Set<SocketAddress> participantNodes) {
    MRConfig.participantNodes = participantNodes;
  }
  
  public static String getConfigFileName() {
    return configFileName;
  }

  public static void setConfigFileName(String configFileName) {
    MRConfig.configFileName = configFileName;
  }
  
  public static int getMaxWorkerRetriesBeforeCancellingJob() {
    return maxWorkerRetriesBeforeCancellingJob;
  }
  
  public static void setMaxWorkerRetriesBeforeCancellingJob(
      int maxWorkerRetriesBeforeCancellingJob) {
    MRConfig.maxWorkerRetriesBeforeCancellingJob = maxWorkerRetriesBeforeCancellingJob;
  }
  
  public static int getTimeToCheckForJobState() {
    return timeToCheckForJobState;
  }
  
  public static void setTimeToCheckForJobState(int timeToCheckForJobState) {
    MRConfig.timeToCheckForJobState = timeToCheckForJobState;
  }
  
  public static int getTimeToCheckForNodeManagerState() {
    return timeToCheckForNodeManagerState;
  }
  
  public static void setTimeToCheckForNodeManagerState(
      int timeToCheckForNodeManagerState) {
    MRConfig.timeToCheckForNodeManagerState = timeToCheckForNodeManagerState;
  }
  
  public static int getWorkersPerNode() {
    return workersPerNode;
  }
  
  public static void setWorkersPerNode(int workersPerNode) {
    MRConfig.workersPerNode = workersPerNode;
  }
  
  public static int getReplicationFactor() {
    return replicationFactor;
  }
  
  public static void setReplicationFactor(int replicationFactor) {
    MRConfig.replicationFactor = replicationFactor;
  }
  
  public static int getChunkSizeInBytes() {
    return chunkSizeInBytes;
  }
  
  public static void setChunkSizeInBytes(int chunkSize) {
    MRConfig.chunkSizeInBytes = chunkSize;
  }
  
  public static int getMaxDFSReadRetries() {
    return maxDFSReadRetries;
  }
  
  public static void setMaxDFSReadRetries(int maxDFSReadRetries) {
    MRConfig.maxDFSReadRetries = maxDFSReadRetries;
  }

  public static int getTimeToCheckDataNodesState() {
    return timeToCheckDataNodesState;
  }
  
  public static void setTimeToCheckDataNodesState(int timeToCheckDataNodesState) {
    MRConfig.timeToCheckDataNodesState = timeToCheckDataNodesState;
  }
  
  private static boolean isValid() {
    return getMasterSocketAddress() != null
        && getParticipantNodes() != null
        && getConfigFileName() != null
        && getMaxWorkerRetriesBeforeCancellingJob() > 0
        && getReplicationFactor() > 0
        && getTimeToCheckDataNodesState() > 0
        && getTimeToCheckForNodeManagerState() > 0
        && getTimeToCheckForJobState() > 0
        && getWorkersPerNode() > 0;
  }
  
  public static boolean setupFromConfigFile(String configFilePath) {
    try {
      MRConfigFromJSONCreator.setupMRConfigFromJSONFile(new File(configFilePath));
    } catch (FileNotFoundException e1) {
      LOG.error("Failed to read config file", e1);
      return false;
    } catch (IOException e1) {
      LOG.error("Failed to read config file", e1);
      return false;
    }
    boolean valid = isValid();
    if(valid) {
      LOG.info(String.format("Node set up with params: \n" +
      		"\tMaster socket address %s\n\tParticipant nodes %s\n\t" +
      		"Worker retries: %d\n\tTime to check job state: %d\n\t" +
      		"Time to check NM state: %d\n\tWorkers per node:%d\n\t" +
      		"Replication factor: %d\n\tChunk size: %d\n\t" +
      		"Max DFS read retries: %d\n\tTime to check data nodes: %d", 
      		masterSocketAddress, participantNodes, maxWorkerRetriesBeforeCancellingJob,
      		timeToCheckForJobState, timeToCheckForNodeManagerState, workersPerNode,
      		replicationFactor, chunkSizeInBytes, maxDFSReadRetries, 
      		timeToCheckDataNodesState));
    } 
    return isValid();
  }

}
