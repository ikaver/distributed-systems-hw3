package com.ikaver.aagarwal.hw3.mrdfs.master;

import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.ikaver.aagarwal.hw3.common.config.MRConfig;
import com.ikaver.aagarwal.hw3.common.definitions.Definitions;
import com.ikaver.aagarwal.hw3.common.dfs.FileMetadata;
import com.ikaver.aagarwal.hw3.common.dfs.FileUtil;
import com.ikaver.aagarwal.hw3.common.dfs.IDFS;
import com.ikaver.aagarwal.hw3.common.dfs.IDataNode;
import com.ikaver.aagarwal.hw3.common.util.Pair;
import com.ikaver.aagarwal.hw3.common.util.SocketAddress;
import com.ikaver.aagarwal.hw3.mrdfs.datanode.DataNodeFactory;

/**
 * The DFS master is responsible for listening for write requests of nodes.
 *  Whenever a node requests the DFS master to write a file, 
 *  the DFS saves the file in several data nodes (the amount of nodes is 
 *  given by the replication factor parameter) and keeps track of 
 *  which node has each file. Whenever a data node goes down, the DFS 
 *  master replicates all of the files that were on the node to the other 
 *  data nodes currently on the system. Additionally, the DFS master also 
 *  responds for "name" requests, basically, it tells the clients in which 
 *  node can they find the file that they're looking for.
 */
@Singleton
public class DFSImpl extends UnicastRemoteObject implements IDFS, IOnDataNodeFailureHandler {

  private static final long serialVersionUID = 5494800394142393419L;

  private static final Logger LOG = LogManager.getLogger(DFSImpl.class);

  private Set<SocketAddress> dataNodes;
  private Map<String, FileMetadata> filePathToMetadata;
  private ReadWriteLock dataNodesLock;
  private ReadWriteLock metadataLock;
  transient private ScheduledExecutorService nodeTrackerService;
  private int replicationFactor;

  @Inject
  public DFSImpl(
      @Named(Definitions.DFS_REPLICATION_FACTOR_ANNOTATION) Integer replicationFactor, 
      @Named(Definitions.DFS_MAP_FILE_TO_METADATA_ANNOTATION) Map<String, FileMetadata> fileToMetadata,
      @Named(Definitions.DFS_DATA_NODES_ANNOTATION) Set<SocketAddress> dataNodes,
      @Named(Definitions.DFS_DATA_NODES_SET_LOCK_ANNOTATION) ReadWriteLock dataNodesLock,
      @Named(Definitions.DFS_MAP_FILE_TO_METADATA_LOCK_ANNOTATION)ReadWriteLock metadataLock) throws RemoteException {
    super();
    this.filePathToMetadata = fileToMetadata;
    this.replicationFactor = replicationFactor;
    this.dataNodes = dataNodes;
    this.dataNodesLock = dataNodesLock;
    this.metadataLock = metadataLock;
    //Start node tracker service (keeps track of node performance).
    this.nodeTrackerService = Executors.newScheduledThreadPool(1);
    DataNodeTracker tracker = new DataNodeTracker(dataNodes, dataNodesLock, this);
    this.nodeTrackerService.scheduleAtFixedRate(tracker, 0,
        MRConfig.getTimeToCheckDataNodesState(), TimeUnit.SECONDS);
  }

  public FileMetadata getMetadata(String file) throws RemoteException {
    this.metadataLock.readLock().lock();
    FileMetadata metadata = this.filePathToMetadata.get(file);
    if(metadata == null) LOG.warn("File metadata for file " + file + " is null");
    this.metadataLock.readLock().unlock();
    return metadata;
  }

  public boolean containsFile(String filePath) throws RemoteException {
    return getMetadata(filePath) != null;
  }

  public boolean createFile(String filePath, int recordSize, long totalFileSize)
      throws RemoteException {
    this.metadataLock.writeLock().lock();

    boolean success = false;
    if(this.filePathToMetadata.containsKey(filePath)) {
      LOG.warn(filePath +  " already exists");
    }
    int numChunks = FileUtil.numChunksForFile(MRConfig.getChunkSizeInBytes(),
        recordSize, totalFileSize);
    Map<Integer, Set<SocketAddress>> numChunkToAddr 
    = new HashMap<Integer, Set<SocketAddress>>();
    FileMetadata metadata = new FileMetadata(filePath, numChunks, 
        numChunkToAddr, recordSize, totalFileSize);
    this.filePathToMetadata.put(filePath, metadata);
    success = true;
    this.metadataLock.writeLock().unlock();
    return success;
  }

  public boolean saveFile(String filePath, int numChunk, byte[] file)
      throws RemoteException {
    this.metadataLock.readLock().lock();
    boolean success = false;
    FileMetadata metadata = this.filePathToMetadata.get(filePath);
    this.metadataLock.readLock().unlock();
    if(metadata != null) {
      success = this.writeNewFile(metadata, numChunk, file);
    }
    return success;
  }

  public byte [] getFile(String filePath, int numChunk) throws RemoteException {
    byte [] data = null;
    this.metadataLock.readLock().lock();
    FileMetadata metadata = this.filePathToMetadata.get(filePath);
    this.metadataLock.readLock().unlock();

    Set<SocketAddress> dataNodesAddr = metadata.getNumChunkToAddr().get(numChunk);
    SocketAddress addr = getRandomDataNode(dataNodesAddr);
    IDataNode datanode = DataNodeFactory
        .dataNodeFromSocketAddress(addr);
    if(datanode == null) return null;
    try {
      data = datanode.getFile(filePath, numChunk);
    } 
    catch(RemoteException e) {
      LOG.warn("Failed to read file from data node", e);
    }
    catch (IOException e) {
      LOG.warn("Failed to read file from data node", e);
    }
    return data;
  }

  private SocketAddress getRandomDataNode(Set<SocketAddress> dataNodes) {
    if(dataNodes == null || dataNodes.size() == 0) return null;
    List<SocketAddress> list = new ArrayList<SocketAddress>(dataNodes);
    Collections.shuffle(list);
    return list.get(0);
  } 

  /**
   * Writes the new file with path filePath and contents file.
   * Assumes that you currently have the write lock.
   * @param filePath
   * @param file
   * @return true iff write successful
   */
  private boolean writeNewFile(FileMetadata metadata, int numChunk, byte [] file) {
    Set<SocketAddress> dataNodesForFile = dataNodesForNewFile();
    boolean saveSuccessful = writeFileInDataNodes(metadata, numChunk, file,
        dataNodesForFile);
    if(saveSuccessful) {
      metadata.getNumChunkToAddr().put(numChunk, dataNodesForFile);
      LOG.info(String.format("Saved file (%s %d) successfully", 
          metadata.getFileName(), numChunk));
    }
    else {
      LOG.warn(String.format("Failed to save file (%s %d)", 
          metadata.getFileName(), numChunk));
    }
    return saveSuccessful;
  }

  private boolean writeFileInDataNodes(FileMetadata metadata, int numChunk, byte [] file, Set<SocketAddress> dataNodes) {
    boolean savedAtLeastInOneDataNode = false;
    for(SocketAddress addr : dataNodes) {
      IDataNode dataNode = DataNodeFactory.dataNodeFromSocketAddress(addr);
      if(dataNode != null) {
        try {
          LOG.info(String.format("Will send file: (%s, %d) to data node %s",
              metadata.getFileName(), numChunk, addr));
          dataNode.saveFile(metadata.getFileName(), numChunk, file);
          LOG.info(String.format("File (%s, %d) was sent to data node %s",
              metadata.getFileName(), numChunk, addr));
          savedAtLeastInOneDataNode = true;
        } catch (IOException e) {
          LOG.warn("Failed to write file on data node", e);
        }
      }
    }  
    return savedAtLeastInOneDataNode;
  }

  private Set<SocketAddress> dataNodesForNewFile() {
    this.dataNodesLock.readLock().lock();
    List<SocketAddress> dataNodesList = new ArrayList<SocketAddress>(this.dataNodes);
    this.dataNodesLock.readLock().unlock();
    Collections.shuffle(dataNodesList);
    Set<SocketAddress> subset = new HashSet<SocketAddress>();
    for(int i = 0; i < Math.min(replicationFactor, dataNodesList.size()); ++i) {
      subset.add(dataNodesList.get(i));
    }
    return subset;
  }

  public void onDataNodeFailed(SocketAddress addr) {
    LOG.info("Data node at addr: " + addr + " failed.");
    this.dataNodesLock.readLock().lock();
    //list of nodes that are still working (we will replicate files on some of
    //this nodes).
    List<SocketAddress> dataNodesList = new ArrayList<SocketAddress>(this.dataNodes);
    this.dataNodesLock.readLock().unlock();
    //shuffle the list so we don't store the same file on the same data node.
    Collections.shuffle(dataNodesList);

    //make a map metadata --> num chunk of all of the files that need to be replicated
    List<Pair<FileMetadata, Integer>> filesThatNeedToBeReplicated = new ArrayList<Pair<FileMetadata, Integer>>();

    //get all files that were were on the data node that failed.
    this.metadataLock.writeLock().lock();
    for(FileMetadata metadata : this.filePathToMetadata.values()) {
      for(int numChunk : metadata.getNumChunkToAddr().keySet()) {
        Set<SocketAddress> nodesForChunk = metadata.getNumChunkToAddr().get(numChunk);
        if(nodesForChunk.contains(addr)) {
          filesThatNeedToBeReplicated.add(new Pair<FileMetadata, Integer>(metadata, numChunk));
          nodesForChunk.remove(addr);
        }
      }
    }
    this.metadataLock.writeLock().unlock();

    //get copy of files and write them on new data node
    for(Pair<FileMetadata, Integer> pair : filesThatNeedToBeReplicated) {
      FileMetadata metadata = pair.first;
      Integer chunkNum = pair.second;
      //which nodes have this data?
      Set<SocketAddress> nodesWithData = metadata.getNumChunkToAddr().get(chunkNum);
      byte [] data = null;

      //1. get the file from the other nodes that have the file
      for(SocketAddress nodeWithDataAddr : nodesWithData) {
        IDataNode nodeWithData = DataNodeFactory.dataNodeFromSocketAddress(nodeWithDataAddr);
        if(nodeWithData == null) continue;
        try {
          data = nodeWithData.getFile(metadata.getFileName(), chunkNum);
          if(data != null) break;
        } catch (RemoteException e) {
          LOG.warn("Failed to communicate with data node", e);
        } catch (IOException e) {
          LOG.warn("Failed to read file from data node", e);
        }
      }

      if(data == null) {
        //failed to get the file from all of the nodes that actually have it
        //continue with next file that needs to be replicated
        LOG.warn("Failed to replicate data for chunk: " + metadata.getFileName()
            + " " + chunkNum);
        continue;
      }

      //We got the data! now try to save it on a new node
      for(SocketAddress newDataNodeAddr : dataNodesList) {
        if(!nodesWithData.contains(newDataNodeAddr)) {
          //now we know that newDataNodeAddr doesn't have the file, so we
          //can replicate it here
          IDataNode newDataNode = DataNodeFactory.dataNodeFromSocketAddress(newDataNodeAddr);
          if(newDataNode == null) continue; //try with the next node
          try {
            LOG.info("Will try to replicate file " + metadata.getFileName() + " "
                + chunkNum + " on host: "  + newDataNodeAddr);
            newDataNode.saveFile(metadata.getFileName(), chunkNum, data);
            this.metadataLock.writeLock().lock();
            nodesWithData.add(newDataNodeAddr);
            this.metadataLock.writeLock().unlock();
            LOG.info("Successfully replicated " + metadata.getFileName() + " "
                + chunkNum + " on host: "  + newDataNodeAddr);
            break;
          } catch (RemoteException e) {
            LOG.warn("Failed to communicate with data node", e);
          } catch (IOException e) {
            LOG.warn("Failed to read file from data node", e);
          }
        }
      }
    }
  }

}
