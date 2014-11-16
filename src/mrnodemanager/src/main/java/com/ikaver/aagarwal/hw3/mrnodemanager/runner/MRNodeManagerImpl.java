package com.ikaver.aagarwal.hw3.mrnodemanager.runner;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.ikaver.aagarwal.hw3.common.definitions.Definitions;
import com.ikaver.aagarwal.hw3.common.dfs.FileMetadata;
import com.ikaver.aagarwal.hw3.common.dfs.FileUtil;
import com.ikaver.aagarwal.hw3.common.dfs.IDFS;
import com.ikaver.aagarwal.hw3.common.dfs.IDataNode;
import com.ikaver.aagarwal.hw3.common.mrmap.IMapInstanceRunner;
import com.ikaver.aagarwal.hw3.common.nodemanager.IMRNodeManager;
import com.ikaver.aagarwal.hw3.common.nodemanager.NodeState;
import com.ikaver.aagarwal.hw3.common.util.FileOperationsUtil;
import com.ikaver.aagarwal.hw3.common.util.SocketAddress;
import com.ikaver.aagarwal.hw3.common.workers.MapWorkDescription;
import com.ikaver.aagarwal.hw3.common.workers.MapperChunk;
import com.ikaver.aagarwal.hw3.common.workers.MapperOutput;
import com.ikaver.aagarwal.hw3.common.workers.ReduceWorkDescription;
import com.ikaver.aagarwal.hw3.common.workers.WorkerState;
import com.ikaver.aagarwal.hw3.mrdfs.datanode.DataNodeFactory;
import com.ikaver.aagarwal.hw3.mrdfs.master.DFSFactory;

/**
 * A task manager manages a "slave" node. Following are the responsibilities of
 * a task manager. 1. Accepts a jar and forks out separate jvms for running
 * mapper and reducer tasks. 2. Periodically updates master with the status of
 * the map reduce job assigned to it.
 */
public class MRNodeManagerImpl extends UnicastRemoteObject implements
		IMRNodeManager {

	private final Logger LOG = Logger.getLogger(MRNodeManagerImpl.class);
	
	// Work in progress is a map between parition id and port at which the mapper task
	// is running.
	private Map<MapWorkDescription, Integer> workInProgress;

	private final SocketAddress masterAddress;

	@Inject
	public MRNodeManagerImpl(
			@Named(Definitions.MASTER_SOCKET_ADDR_ANNOTATION) SocketAddress masterAddress)
			throws RemoteException {
		super();
		this.masterAddress = masterAddress;
		this.workInProgress = new ConcurrentHashMap<MapWorkDescription, Integer> ();
	}

	private static final long serialVersionUID = 1674990898801584371L;
	private static final Logger LOGGER = Logger
			.getLogger(MRNodeManagerImpl.class);

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
				+ input.getChunk().getInputFilePath()
				+ " for the partition" + input.getChunk().getPartitionID());
		String inputPath = input.getChunk().getInputFilePath();
		// partition id is chunk id for now.
		int chunk = input.getChunk().getPartitionID();

		byte[] data = fetchData(inputPath, chunk);

		if (data == null) {
			LOG.warn("Error fetching data from dfs for " + inputPath
					+ " for chunk" + chunk);
			return false;
		}

		String localfp = writeDataToLocalPath(data);

		int port = MRMapTaskAttempt.startMapTask(input);

		if (port == -1) {
			LOGGER.warn("error starting work instance");
			return false;
		}
		
		workInProgress.put(input, port);

		LOGGER.info(String.format("Starting map runner at port: %d", port));
		try {
			IMapInstanceRunner runner = (IMapInstanceRunner) Naming
					.lookup(String.format("//%s:%d/%s", "localhost", port,
							Definitions.MR_MAP_RUNNER_SERVICE));

			runner.runMapInstance(input, localfp);
		} catch (MalformedURLException e) {
			return false;
		} catch (RemoteException e) {
			return false;
		} catch (NotBoundException e) {
			return false;
		}
		return true;
	}

	private String writeDataToLocalPath(byte[] data) {
		String localfp = FileOperationsUtil.getRandomStringForLocalFile();
		FileOutputStream os;
		try {
			os = new FileOutputStream(new File(localfp));
			os.write(data);
			os.close();
			FileUtil.changeFilePermission(localfp);
			return localfp;
		} catch (FileNotFoundException e) {
			LOG.warn("error writing to the file." + localfp, e);
		} catch (IOException e) {
			LOG.warn("IO exception while writing to the file.", e);
		}
		return null;
	}

	/**
	 * Fetches data from DFS
	 * @param inputPath is a path on the dfs.
	 * @param chunk is the chunk which needs to be fetched.
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

				if (preferredNode != null) {
					preferredNode = getRandomDataNode(datanodes);
				}
				IDataNode datanode = DataNodeFactory
						.dataNodeFromSocketAddress(preferredNode);

				byte[] data = datanode.getFile(inputPath, chunk);
				return data;
			} catch (RemoteException e) {
				LOG.warn("Remote exception while reading data.", e);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				LOG.warn("Error while fetching data from the disk.", e);
			}
		}

		return null;
	}

	private SocketAddress getRandomDataNode(Set<SocketAddress> datanodes) {
		List<SocketAddress> list = new ArrayList<SocketAddress>(datanodes);
		Collections.shuffle(list);
		return list.get(0);
	}

	private SocketAddress getPreferredAddress(Set<SocketAddress> addresses) {
		for (SocketAddress address : addresses) {
			try {
				LOGGER.info("Checking if " +  address.getHostname()
						+ " " + InetAddress.getLocalHost().getHostName() + "matches..");
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

	public boolean doReduce(ReduceWorkDescription input) throws RemoteException {
		throw new UnsupportedOperationException("Not yet implemented :(");
	}

	public <K extends Serializable & Comparable<K>, V extends Serializable> List<MapperOutput<K, V>> dataForJob(
			int jobID, MapperChunk chunk, int reducerID) {
		throw new UnsupportedOperationException("Not yet implemented :(");
	}

	public NodeState getNodeState() {
		throw new UnsupportedOperationException("Not yet implemented :(");
	}

	public WorkerState getMapperState(int jobId, int partitionId) {
		throw new UnsupportedOperationException("Not yet implemented :(");
	}

	public WorkerState getReducerState(int jobId, int reducerId) {
		throw new UnsupportedOperationException("Not yet implemented :(");
	}

	public boolean terminateWorkers(int jobID) {
		throw new UnsupportedOperationException("Not yet implemented :(");
	}

	public void startReducerWork(int jobID, int reducerID) {
		throw new UnsupportedOperationException("Not yet implemented :(");
	}

  public int getAvailableSlots() throws RemoteException {
    return 3;
  }

  public void updateMappersReferences(List<SocketAddress> mapperAddr,
      List<MapperChunk> chunks) throws RemoteException {
    throw new UnsupportedOperationException("Not yet implemented :(");    
  }
}
