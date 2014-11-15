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
import java.util.concurrent.locks.ReadWriteLock;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.ikaver.aagarwal.hw3.common.definitions.Definitions;
import com.ikaver.aagarwal.hw3.common.dfs.FileMetadata;
import com.ikaver.aagarwal.hw3.common.dfs.FileUtil;
import com.ikaver.aagarwal.hw3.common.dfs.IDFS;
import com.ikaver.aagarwal.hw3.common.dfs.IDataNode;
import com.ikaver.aagarwal.hw3.common.util.Pair;
import com.ikaver.aagarwal.hw3.common.util.SocketAddress;
import com.ikaver.aagarwal.hw3.mrdfs.datanode.DataNodeFactory;

public class DFSImpl extends UnicastRemoteObject implements IDFS, IOnDataNodeFailureHandler {

  private static final long serialVersionUID = 5494800394142393419L;

  private static final Logger LOG = LogManager.getLogger(DFSImpl.class);

  private Set<SocketAddress> dataNodes;
  private Map<String, FileMetadata> filePathToMetadata;
  private ReadWriteLock dataNodesLock;
  private ReadWriteLock metadataLock;
  private int replicationFactor;

  @Inject
  public DFSImpl(
      @Named(Definitions.DFS_REPLICATION_FACTOR_ANNOTATION) Integer replicationFactor, 
      @Named(Definitions.DFS_MAP_FILE_TO_METADATA_ANNOTATION) Map<String, FileMetadata> fileToMetadata,
      @Named(Definitions.DFS_DATA_NODES_ANNOTATION) Set<SocketAddress> dataNodes,
      @Named(Definitions.DFS_DATA_NODES_SET_LOCK_ANNOTATION) ReadWriteLock dataNodesLock) throws RemoteException {
    super();
    this.filePathToMetadata = fileToMetadata;
    this.replicationFactor = replicationFactor;
    this.dataNodes = dataNodes;
    this.dataNodesLock = dataNodesLock;
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
    else {
      int numChunks = FileUtil.numChunksForFile(Definitions.SIZE_OF_CHUNK,
          recordSize, totalFileSize);
      Map<Integer, Set<SocketAddress>> numChunkToAddr 
      = new HashMap<Integer, Set<SocketAddress>>();
      FileMetadata metadata = new FileMetadata(filePath, numChunks, 
          numChunkToAddr, recordSize, totalFileSize);
      this.filePathToMetadata.put(filePath, metadata);
      success = true;
    }
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
      this.writeNewFile(metadata, numChunk, file);
    }
    return success;
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
    }
    return saveSuccessful;
  }

  private boolean writeFileInDataNodes(FileMetadata metadata, int numChunk, byte [] file, Set<SocketAddress> dataNodes) {
    boolean savedAtLeastInOneDataNode = false;
    for(SocketAddress addr : dataNodes) {
      IDataNode dataNode = DataNodeFactory.dataNodeFromSocketAddress(addr);
      if(dataNode != null) {
        try {
          dataNode.saveFile(metadata.getFileName(), numChunk, file);
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
    for(int i = 0; i < replicationFactor; ++i) {
      subset.add(dataNodesList.get(i));
    }
    return subset;
  }

  public void onDataNodeFailed(SocketAddress addr) {
    this.dataNodesLock.readLock().lock();
    //list of nodes that are still working (we will replicate files on some of
    //this nodes).
    List<SocketAddress> dataNodesList = new ArrayList<SocketAddress>(this.dataNodes);
    this.dataNodesLock.readLock().unlock();
    Collections.shuffle(dataNodesList);

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
      Set<SocketAddress> nodesWithData = metadata.getNumChunkToAddr().get(chunkNum);
      byte [] data = null;
      for(SocketAddress nodeWithDataAddr : nodesWithData) {
        IDataNode nodeWithData = DataNodeFactory.dataNodeFromSocketAddress(nodeWithDataAddr);
        if(nodeWithData == null) continue;
        try {
          data = nodeWithData.getFile(metadata.getFileName(), chunkNum);
          break;
        } catch (RemoteException e) {
          LOG.warn("Failed to communicate with data node", e);
        } catch (IOException e) {
          LOG.warn("Failed to read file from data node", e);
        }
      }
      if(data == null) {
        LOG.warn("Failed to replicate data for chunk: " + metadata.getFileName()
            + " " + chunkNum);
        continue;
      }
      for(SocketAddress newDataNodeAddr : dataNodesList) {
        if(!nodesWithData.contains(newDataNodeAddr)) {
          IDataNode newDataNode = DataNodeFactory.dataNodeFromSocketAddress(newDataNodeAddr);
          if(newDataNode == null) continue;
          try {
            newDataNode.saveFile(metadata.getFileName(), chunkNum, data);
            this.metadataLock.writeLock().lock();
            nodesWithData.add(newDataNodeAddr);
            this.metadataLock.writeLock().unlock();
          } catch (RemoteException e) {
            LOG.warn("Failed to communicate with data node", e);
          } catch (IOException e) {
            LOG.warn("Failed to read file from data node", e);
          }
          if(data == null) continue;
        }
      }
    }
  }

}
