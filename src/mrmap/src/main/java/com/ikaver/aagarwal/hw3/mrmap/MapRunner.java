package com.ikaver.aagarwal.hw3.mrmap;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.UnknownHostException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import com.ikaver.aagarwal.hw3.common.definitions.Definitions;
import com.ikaver.aagarwal.hw3.common.dfs.DFSFactory;
import com.ikaver.aagarwal.hw3.common.dfs.FileMetadata;
import com.ikaver.aagarwal.hw3.common.dfs.IDFS;
import com.ikaver.aagarwal.hw3.common.dfs.IDataNode;
import com.ikaver.aagarwal.hw3.common.mrcollector.ICollector;
import com.ikaver.aagarwal.hw3.common.util.FileOperationsUtil;
import com.ikaver.aagarwal.hw3.common.util.SocketAddress;
import com.ikaver.aagarwal.hw3.common.workers.IMapper;
import com.ikaver.aagarwal.hw3.common.workers.MapWorkDescription;
import com.ikaver.aagarwal.hw3.common.workers.WorkerState;
import com.ikaver.aagarwal.hw3.mrdfs.datanode.DataNodeFactory;

public class MapRunner implements Runnable {

  private static final Logger LOGGER = Logger.getLogger(MapRunner.class);

  private final MapOutputCollector moc;
  private final MapWorkState mapWorkState;
  private MapWorkDescription input;
  private final SocketAddress masterAddress;

  public MapRunner(MapWorkDescription input, SocketAddress masterAddress) {
    this.input = input;
    this.mapWorkState = new MapWorkState();
    this.moc = new MapOutputCollector();
    this.masterAddress = masterAddress;
  }

  public void run() {
    LOGGER.info("Received a map request for "
        + input.getChunk().getInputFilePath() + " for the partition"
        + input.getChunk().getPartitionID());
    String inputPath = input.getChunk().getInputFilePath();
    // partition id is chunk id for now.
    int chunk = input.getChunk().getPartitionID();

    byte[] data = fetchData(inputPath, chunk);

    // TODO(ankit): Remove this weird I/O. why? This is redundant
    // and is a legacy of a bad design choice which I made.
    if (data == null) {
      LOGGER.warn("Error fetching data from dfs for " + inputPath
          + " for chunk" + chunk);
      return;
    }

    String localfp = FileOperationsUtil.storeLocalFile(data, ".input");
    IMapper mapper = getMapperClass(input);

    FileInputStream fis;

    try {
      fis = new FileInputStream(new File(localfp));
    } catch (FileNotFoundException e) {
      LOGGER.fatal("Mapper was expecting" + localfp
          + " to be accessible.");
      mapWorkState.setState(WorkerState.FAILED);
      return;
    }

    mapWorkState.setState(WorkerState.RUNNING);

    byte[] record = new byte[input.getChunk().getRecordSize()];
    try {
      while (fis.read(record) != -1) {
        String recordStr = new String(record);
        try {
          mapper.map(recordStr, (ICollector) moc);
        } catch (Exception e) {
          mapWorkState.setState(WorkerState.FAILED);
          return;
        }
      }

      // Set the output path before you set the state. Otherwise,
      // it can lead to a subtle concurrency bug where you try to
      // access the path which has not yet been set while the map
      // state has been set.
      mapWorkState.setOutputPath(moc.flush());

      // Flush the data to a file and return the path where it is
      // being stored.
      if (mapWorkState.getOutputPath() == null) {
        LOGGER.warn("Failed to get output path. Mapper failed.");
        mapWorkState.setState(WorkerState.FAILED);
      } else {
        LOGGER.info("Mapper finished.");
        mapWorkState.setState(WorkerState.FINISHED);
      }

    } catch (IOException e) {
      mapWorkState.setState(WorkerState.FAILED);
      LOGGER.fatal("error reading data from the local file system. Check"
          + "that the file is accessible.");
    }
  }

  public MapOutputCollector getMoc() {
    return moc;
  }

  public MapWorkState getMapWorkState() {
    return mapWorkState;
  }

  public MapWorkDescription getInput() {
    return input;
  }

  private SocketAddress getPreferredAddress(Set<SocketAddress> addresses) {
    for (SocketAddress address : addresses) {
      try {
        LOGGER.info("Checking if " + address.getHostname() + " "
            + InetAddress.getLocalHost().getHostName()
            + "matches..");
        if (address.getHostname().equals(
            InetAddress.getLocalHost().getHostName())) {
          return address;
        }
      } catch (UnknownHostException e) {
        LOGGER.warn("Error looking up hostname.");
      }
    }
    return null;
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
        if(dfs == null) return null;
        FileMetadata metadata = dfs.getMetadata(inputPath);

        // Get the list of dataodes corresponding to the chunk.
        Set<SocketAddress> datanodes = metadata.getNumChunkToAddr().get(chunk);

        SocketAddress preferredNode = getPreferredAddress(datanodes);

        if (preferredNode == null) {
          preferredNode = getRandomDataNode(datanodes);
        }

        IDataNode datanode = DataNodeFactory.dataNodeFromSocketAddress(preferredNode);

        byte[] data = null;
        if (datanode != null)
          data = datanode.getFile(inputPath, chunk);
        return data;
      } catch (RemoteException e) {
        LOGGER.warn("Remote exception while reading data.", e);
      } catch (IOException e) {
        LOGGER.warn("Error while fetching data from the dfs.", e);
      }
    }

    return null;
  }

  private IMapper getMapperClass(MapWorkDescription input) {
    IMapper mapper = null;
    String jarPath = FileOperationsUtil.storeLocalFile(input.getJarFile(),
        ".jar");
    try {
      File file = new File(jarPath);
      ClassLoader loader = new URLClassLoader(new URL[] { file.toURI()
          .toURL() });
      Class<IMapper> mapperClass = (Class<IMapper>) loader
          .loadClass(input.getMapperClass());
      mapper = mapperClass.newInstance();
    } catch (IOException e) {
      LOGGER.fatal("Error reading jar file from the disk. Either the"
          + "file" + jarPath
          + "doesn't exist on the local filesystem"
          + " or the local disk is full");
    } catch (ClassNotFoundException e) {
      LOGGER.fatal("Unable to locate the mapper class"
          + input.getMapperClass());
    } catch (InstantiationException e) {
      LOGGER.fatal("Error instanting the mapper class"
          + input.getMapperClass());
    } catch (IllegalAccessException e) {
    }
    return mapper;
  }

  private SocketAddress getRandomDataNode(Set<SocketAddress> datanodes) {
    if (datanodes == null || datanodes.size() == 0)
      return null;
    List<SocketAddress> list = new ArrayList<SocketAddress>(datanodes);
    Collections.shuffle(list);
    return list.get(0);
  }

}
