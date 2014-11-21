package com.ikaver.aagarwal.hw3.common.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import com.ikaver.aagarwal.hw3.common.util.SocketAddress;

public class MRConfigFromJSONCreator {
  
  private static final Logger LOG = Logger.getLogger(MRConfigFromJSONCreator.class);
    
  private static final String CONFIG_OBJECT = "config";
  private static final String MASTER_IP = "master-host";
  private static final String MASTER_PORT = "master-port";
  private static final String PARTICIPANTS_ARRAY = "participants";
  private static final String PARTICIPANT_IP = "ip";
  private static final String PARTICIPANT_PORT = "port";
  private static final String MAX_RETRIES_BEFORE_JOB_FAIL = "max-retries-before-job-failure";
  private static final String TIME_TO_CHECK_JOB_STATE = "time-to-check-for-job-state";
  private static final String TIME_TO_CHECK_NODE_MANAGERS_STATE = "time-to-check-for-nodes-state";
  private static final String TIME_TO_CHECK_DATA_NODES_STATE = "time-to-check-for-data-nodes-state";
  private static final String WORKERS_PER_NODE = "workers-per-node";
  private static final String REPLICATION_FACTOR = "replication-factor";
  private static final String CHUNK_SIZE_IN_MB = "chunk-size-in-MB";
  private static final String MAX_DFS_READ_RETRIES = "max-dfs-read-retries";

  public static boolean setupMRConfigFromJSONFile(File jsonFile) throws IOException, UnsupportedEncodingException {
    FileInputStream fis = new FileInputStream(jsonFile);
    byte[] data = new byte[(int)jsonFile.length()];
    fis.read(data);
    fis.close();
    String json = new String(data, "UTF-8");
    MRConfig.setConfigFileName(jsonFile.getName());
    return setupMRConfigFromJSON(json);
  }
  
  public static boolean setupMRConfigFromJSON(String json) throws FileNotFoundException, IOException {
    if(json == null) return false;
    JSONObject obj = new JSONObject(json);
    JSONObject config = obj.getJSONObject(CONFIG_OBJECT);
    
    String masterIP = config.getString(MASTER_IP);
    int masterPort = config.getInt(MASTER_PORT);
    SocketAddress masterSocketAddress = new SocketAddress(masterIP, masterPort);
    Set<SocketAddress> participantsSocketAddress = new HashSet<SocketAddress>();
    JSONArray participantsArray = config.getJSONArray(PARTICIPANTS_ARRAY);
    for(int i = 0; i < participantsArray.length(); ++i) {
      JSONObject participantObj = participantsArray.getJSONObject(i);
      String host = participantObj.getString(PARTICIPANT_IP);
      int port = participantObj.getInt(PARTICIPANT_PORT);
      participantsSocketAddress.add(new SocketAddress(host, port));
    }
    int maxRetriesBeforeJobFail = config.getInt(MAX_RETRIES_BEFORE_JOB_FAIL);
    int timeToCheckJobState = config.getInt(TIME_TO_CHECK_JOB_STATE);
    int timeToCheckNMState = config.getInt(TIME_TO_CHECK_NODE_MANAGERS_STATE);
    int timeToCheckDataNodeState = config.getInt(TIME_TO_CHECK_DATA_NODES_STATE);
    int workersPerNode = config.getInt(WORKERS_PER_NODE);
    int replicationFactor = config.getInt(REPLICATION_FACTOR);
    int chunkSizeInBytes = config.getInt(CHUNK_SIZE_IN_MB) << 20;
    int dfsReadRetries = config.getInt(MAX_DFS_READ_RETRIES);
    
    MRConfig.setMasterSocketAddress(masterSocketAddress);
    MRConfig.setParticipantNodes(participantsSocketAddress);
    MRConfig.setMaxWorkerRetriesBeforeCancellingJob(maxRetriesBeforeJobFail);
    MRConfig.setTimeToCheckForJobState(timeToCheckJobState);
    MRConfig.setTimeToCheckForNodeManagerState(timeToCheckNMState);
    MRConfig.setTimeToCheckDataNodesState(timeToCheckDataNodeState);
    MRConfig.setWorkersPerNode(workersPerNode);
    MRConfig.setReplicationFactor(replicationFactor);
    MRConfig.setChunkSizeInBytes(chunkSizeInBytes);
    MRConfig.setMaxDFSReadRetries(dfsReadRetries);
    
    return true;
  }

}
