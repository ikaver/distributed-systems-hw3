package com.ikaver.aagarwal.hw3.mrnodemanager.runner;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.ikaver.aagarwal.hw3.common.definitions.Definitions;
import com.ikaver.aagarwal.hw3.common.dfs.DFSFactory;
import com.ikaver.aagarwal.hw3.common.dfs.FileMetadata;
import com.ikaver.aagarwal.hw3.common.dfs.IDFS;
import com.ikaver.aagarwal.hw3.common.dfs.IDataNode;
import com.ikaver.aagarwal.hw3.common.mrmap.IMapInstanceRunner;
import com.ikaver.aagarwal.hw3.common.mrreduce.IMRReduceInstanceRunner;
import com.ikaver.aagarwal.hw3.common.nodemanager.IMRNodeManager;
import com.ikaver.aagarwal.hw3.common.nodemanager.NodeState;
import com.ikaver.aagarwal.hw3.common.objects.KeyValuePair;
import com.ikaver.aagarwal.hw3.common.util.FileOperationsUtil;
import com.ikaver.aagarwal.hw3.common.util.SocketAddress;
import com.ikaver.aagarwal.hw3.common.workers.MapWorkDescription;
import com.ikaver.aagarwal.hw3.common.workers.MapperChunk;
import com.ikaver.aagarwal.hw3.common.workers.ReduceWorkDescription;
import com.ikaver.aagarwal.hw3.common.workers.WorkerState;
import com.ikaver.aagarwal.hw3.mrdfs.datanode.DataNodeFactory;
import com.ikaver.aagarwal.hw3.mrnodemanager.util.MapInstanceRunnerFactory;

/**
 * A task manager manages a "slave" node. Following are the responsibilities of
 * a task manager. 1. Accepts a jar and forks out separate jvms for running
 * mapper and reducer tasks. 2. Periodically updates master with the status of
 * the map reduce job assigned to it.
 */
public class MRNodeManagerImpl extends UnicastRemoteObject implements
IMRNodeManager {

  private static final long serialVersionUID = 1674990898801584371L;

  private final Logger LOG = Logger.getLogger(MRNodeManagerImpl.class);

  // Work in progress is a map between parition id and port at which the
  // mapper task
  // is running.
  private final Map<MapWorkDescription, Integer> mapWorkDescriptionToPortMapping;
  private final Map<ReduceWorkDescription, Integer> reduceWorkDescriptionToPortMapping;
  private final Map<MapWorkDescription, Integer> runningMappers;
  private final Map<ReduceWorkDescription, Integer> runningReducers;
  private final SocketAddress masterAddress;

  private final ReadWriteLock mappersLock;
  private final ReadWriteLock reducersLock;

  @Inject
  public MRNodeManagerImpl(
      @Named(Definitions.MASTER_SOCKET_ADDR_ANNOTATION) SocketAddress masterAddress)
          throws RemoteException {
    super();
    this.masterAddress = masterAddress;
    this.mapWorkDescriptionToPortMapping = new ConcurrentHashMap<MapWorkDescription, Integer>();
    this.reduceWorkDescriptionToPortMapping = new ConcurrentHashMap<ReduceWorkDescription, Integer>();
    this.runningMappers = new ConcurrentHashMap<MapWorkDescription, Integer>();
    this.runningReducers = new ConcurrentHashMap<ReduceWorkDescription, Integer>();

    this.mappersLock = new ReentrantReadWriteLock();
    this.reducersLock = new ReentrantReadWriteLock();
  }

  /**
   * Following is the sequence of operations which should be executed by the
   * doMap function. 1. Fetch data from DFS and copy to the local disk. 2.
   * Copy the jars from the DFS to the local disk. 3. Pass the local path from
   * (1) and (2) to the mapper task.
   * 
   * @return
   */
  @SuppressWarnings("resource")
  public boolean doMap(MapWorkDescription input) {
    LOG.info("Received a map request for "
        + input.getChunk().getInputFilePath() + " for the partition"
        + input.getChunk().getPartitionID());
    String inputPath = input.getChunk().getInputFilePath();
    // partition id is chunk id for now.
    int chunk = input.getChunk().getPartitionID();

    byte[] data = fetchData(inputPath, chunk);

    if (data == null) {
      LOG.warn("Error fetching data from dfs for " + inputPath
          + " for chunk" + chunk);
      return false;
    }

    String localfp = FileOperationsUtil.storeLocalFile(data, ".input");

    int port = MRMapTaskAttempt.startMapTask(input);

    if (port == -1) {
      LOG.warn("error starting work instance");
      return false;
    }

    this.mappersLock.writeLock().lock();
    mapWorkDescriptionToPortMapping.put(input, port);
    runningMappers.put(input, port);
    this.mappersLock.writeLock().unlock();

    LOG.info(String.format("Starting map runner at port: %d", port));
    IMapInstanceRunner runner = MRMapFactory.mapInstanceFromPort(port);

    if (runner == null) {
      LOG.warn("Could not locate a running map instance at the port");
      return false;
    }

    try {
      runner.runMapInstance(input, localfp);
      return true;
    } catch (RemoteException e) {
      LOG.info("Remote exception while running the map instance");
      return false;
    }
  }

  /**
   * Fetches data from DFS
   * 
   * @param inputPath
   *            is a path on the dfs.
   * @param chunk
   *            is the chunk which needs to be fetched.
   * @return
   */
  private byte[] fetchData(String inputPath, int chunk) {
    int numTries = 0;

    while (numTries < Definitions.NUM_DFS_READ_RETRIES) {
      numTries++;
      try {
        IDFS dfs = DFSFactory.dfsFromSocketAddress(masterAddress);
        FileMetadata metadata = dfs.getMetadata(inputPath);

        // Get the list of dataodes corresponding to the chunk.
        Set<SocketAddress> datanodes = metadata.getNumChunkToAddr()
            .get(chunk);

        SocketAddress preferredNode = getPreferredAddress(datanodes);

        if (preferredNode == null) {
          preferredNode = getRandomDataNode(datanodes);
        }
        IDataNode datanode = DataNodeFactory
            .dataNodeFromSocketAddress(preferredNode);
        byte[] data = null;
        if (datanode != null)
          data = datanode.getFile(inputPath, chunk);
        if (data != null)
          return data;
      } catch (RemoteException e) {
        LOG.warn("Remote exception while reading data.", e);
      } catch (IOException e) {
        LOG.warn("Error while fetching data from the dfs.", e);
      }
    }

    return null;
  }

  public List<KeyValuePair> dataForJob(MapWorkDescription mwd, ReduceWorkDescription rwd) {
    LOG.info("Getting data for job: " + rwd + " " + mwd);
    WorkerState state = getMapperState(mwd);
    if (state != WorkerState.FINISHED) {
      LOG.error("Trying to fetch state from a worker which either has failed,"
          + "or doesn't exist or is yet to complete it's task");
      return null;
    }

    this.mappersLock.readLock().lock();
    Integer portObj = mapWorkDescriptionToPortMapping.get(mwd);
    this.mappersLock.readLock().unlock();

    if(portObj == null) {
      LOG.warn("Mapper " + mwd + " is not running on this node!");
      return null;
    }
    int port = portObj.intValue();
    IMapInstanceRunner mapper = MRMapFactory.mapInstanceFromPort(port);

    try {
      String outputPath = mapper.getMapOutputFilePath();
      LOG.info("Got output file path of mapper: " + outputPath);
      ObjectInputStream os = new ObjectInputStream(new FileInputStream(
          new File(outputPath)));

      List<KeyValuePair> result = new ArrayList<KeyValuePair>();
      List<KeyValuePair> list = (List<KeyValuePair>) os.readObject();

      LOG.info("Mapper output list size is " + list.size());

      for (KeyValuePair kv : list) {
        if (kv.getKey().hashCode() % rwd.getNumReducers() == rwd.getReducerID()) {
          result.add(kv);
        }
      }
      return result;
    } catch (FileNotFoundException e) {
      LOG.error("The local file specified by the mapper doesn't exist.");
      return null;
    } catch (IOException e) {
      LOG.error("Error reading file from the local file system.");
      return null;
    } catch (ClassNotFoundException e) {
      LOG.error("If you notice this in your error logs, something is"
          + "really bad.");
      return null;
    }
  }

  public boolean doReduce(ReduceWorkDescription rwd) throws RemoteException {
    int port = MRReduceTaskAttempt.startReduceTask(
        masterAddress.getHostname(), masterAddress.getPort());

    if (port == -1) {
      LOG.warn("error starting work instance");
      return false;
    }

    this.reducersLock.writeLock().lock();
    reduceWorkDescriptionToPortMapping.put(rwd, port);
    runningReducers.put(rwd, port);
    this.reducersLock.readLock().unlock();

    LOG.info(String.format("Starting reduce runner at port: %d", port));

    IMRReduceInstanceRunner runner = MRReduceFactory
        .reduceInstanceFromPort(port);

    if (runner == null) {
      LOG.warn("Could not locate a running map instance at the port");
      return false;
    }

    try {
      runner.runReduceInstance(rwd);
      return true;
    } catch (RemoteException e) {
      LOG.info("Remote exception while running the map instance");
      return false;
    }
  }

  public NodeState getNodeState() {
    this.mappersLock.readLock().lock();
    int numMappers = runningMappers.size();
    this.mappersLock.readLock().unlock();
    this.reducersLock.readLock().lock();
    int numReducers = runningReducers.size();
    this.reducersLock.readLock().unlock();
    int availableSlots = Definitions.WORKERS_PER_NODE - numMappers - numReducers;
    return new NodeState(numMappers, 
        numReducers, 
        availableSlots,
        Runtime.getRuntime().availableProcessors());
  }

  public WorkerState getMapperState(MapWorkDescription wd) {
    mappersLock.readLock().lock();
    Integer portOfMapper = mapWorkDescriptionToPortMapping.get(wd);
    mappersLock.readLock().unlock();
    if (portOfMapper == null) {
      LOG.info("No instance of mapper state found for"
          + "map work description found corresponding");
      return WorkerState.WORKER_DOESNT_EXIST;
    }

    WorkerState mapperState = WorkerState.FAILED;
    int port = portOfMapper.intValue();
    IMapInstanceRunner mapper = MRMapFactory.mapInstanceFromPort(port);
    if (mapper != null) { 
      try {
        mapperState = mapper.getMapperState();
      } catch (RemoteException e) {
        LOG.warn(String.format("Mapper %s failed (remote exception)",wd));
      }
    }
    if(mapperState == WorkerState.FAILED || mapperState == WorkerState.FINISHED) {
      mappersLock.writeLock().lock();
      this.removeMapper(wd);
      mappersLock.writeLock().unlock();
    }
    return mapperState;
  }

  public WorkerState getReducerState(ReduceWorkDescription wd) {
    reducersLock.readLock().lock();
    Integer portOfReducer = reduceWorkDescriptionToPortMapping.get(wd);
    reducersLock.readLock().unlock();
    if (portOfReducer == null) {
      LOG.info("No instance of mapper state found for"
          + "reduce work description found corresponding");
      return WorkerState.WORKER_DOESNT_EXIST;
    }

    WorkerState reducerState = WorkerState.FAILED;
    int port = portOfReducer.intValue();
    IMRReduceInstanceRunner reducer = MRReduceFactory.reduceInstanceFromPort(port);
    if (reducer != null) { 
      try {
        reducerState = reducer.getReducerState();
      } catch (RemoteException e) {
        LOG.warn(String.format("Mapper %s failed (remote exception)",wd));
      }
    }
    if(reducerState == WorkerState.FAILED || reducerState == WorkerState.FINISHED) {
      reducersLock.writeLock().lock();
      this.removeReducer(wd);
      reducersLock.writeLock().unlock();
    }
    return reducerState;
  }

  public boolean terminateWorkers(int jobID) {
    this.mappersLock.writeLock().lock();
    boolean success = false;
    try{
      Set<MapWorkDescription> mappers = 
          new HashSet<MapWorkDescription>(this.mapWorkDescriptionToPortMapping.keySet());
      for(MapWorkDescription mapperWork : mappers) {
        if(mapperWork.getJobID() == jobID) {
          Integer portObj = this.mapWorkDescriptionToPortMapping.get(mapperWork);
          if(portObj != null) {
            int port = portObj.intValue();
            IMapInstanceRunner mapper = MRMapFactory.mapInstanceFromPort(port);
            if(mapper != null) {
              try {
                mapper.die();
                removeMapper(mapperWork);
                success = true;
              } catch (RemoteException e) {
                LOG.warn("Failed communicating with mapper", e);
              }
            }
          }
        }
      }
    }
    finally{
      this.mappersLock.writeLock().unlock();
    }

    this.reducersLock.writeLock().lock();
    try{
      Set<ReduceWorkDescription> reducers = 
          new HashSet<ReduceWorkDescription>(this.reduceWorkDescriptionToPortMapping.keySet());
      for(ReduceWorkDescription reducerWork : reducers) {
        if(reducerWork.getJobID() == jobID) {
          Integer portObj = this.mapWorkDescriptionToPortMapping.get(reducerWork);
          if(portObj != null) {
            int port = portObj.intValue();
            IMRReduceInstanceRunner reducer = MRReduceFactory.reduceInstanceFromPort(port);
            if(reducer != null) {
              try {
                reducer.die();
                removeReducer(reducerWork);
                success = true;
              } catch (RemoteException e) {
                LOG.warn("Failed communicating with mapper", e);
              }
            }
          }
        }
      }
    }
    finally{
      this.reducersLock.writeLock().unlock();
    }
    return success;
  }

  public void shutdown() throws RemoteException {
    this.mappersLock.writeLock().lock();
    try{
      Set<MapWorkDescription> mappers = 
          new HashSet<MapWorkDescription>(this.mapWorkDescriptionToPortMapping.keySet());
      for(MapWorkDescription mapperWork : mappers) {
        Integer portObj = this.mapWorkDescriptionToPortMapping.get(mapperWork);
        if(portObj != null) {
          int port = portObj.intValue();
          IMapInstanceRunner mapper = MRMapFactory.mapInstanceFromPort(port);
          if(mapper != null) {
            try {
              mapper.die();
              removeMapper(mapperWork);
            } catch (RemoteException e) {
              LOG.warn("Failed communicating with mapper", e);
            }
          }
        }
      }
    }
    finally{
      this.mappersLock.writeLock().unlock();
    }

    this.reducersLock.writeLock().lock();
    try{
      Set<ReduceWorkDescription> reducers = 
          new HashSet<ReduceWorkDescription>(this.reduceWorkDescriptionToPortMapping.keySet());
      for(ReduceWorkDescription reducerWork : reducers) {
        Integer portObj = this.mapWorkDescriptionToPortMapping.get(reducerWork);
        if(portObj != null) {
          int port = portObj.intValue();
          IMRReduceInstanceRunner reducer = MRReduceFactory.reduceInstanceFromPort(port);
          if(reducer != null) {
            try {
              reducer.die();
              removeReducer(reducerWork);
            } catch (RemoteException e) {
              LOG.warn("Failed communicating with mapper", e);
            }
          }

        }
      }
    }
    finally{
      this.reducersLock.writeLock().unlock();
    }
  }

  private SocketAddress getRandomDataNode(Set<SocketAddress> datanodes) {
    List<SocketAddress> list = new ArrayList<SocketAddress>(datanodes);
    Collections.shuffle(list);
    return list.get(0);
  }

  private void removeMapper(MapWorkDescription wd) {
    this.runningMappers.remove(wd);
  }

  private void removeReducer(ReduceWorkDescription wd) {
    this.runningReducers.remove(wd);
  }

  private SocketAddress getPreferredAddress(Set<SocketAddress> addresses) {
    for (SocketAddress address : addresses) {
      try {
        LOG.info("Checking if " + address.getHostname() + " "
            + InetAddress.getLocalHost().getHostName()
            + "matches..");
        if (address.getHostname().equals(
            InetAddress.getLocalHost().getHostName())) {
          return address;
        }
      } catch (UnknownHostException e) {
        LOG.warn("Error looking up hostname.");
      }
    }
    return null;
  }

}
