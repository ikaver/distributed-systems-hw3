package com.ikaver.aagarwal.hw3.mrreduce;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.ikaver.aagarwal.hw3.common.dfs.DFSFactory;
import com.ikaver.aagarwal.hw3.common.dfs.IDFS;
import com.ikaver.aagarwal.hw3.common.nodemanager.IMRNodeManager;
import com.ikaver.aagarwal.hw3.common.nodemanager.NodeManagerFactory;
import com.ikaver.aagarwal.hw3.common.objects.KeyValuePair;
import com.ikaver.aagarwal.hw3.common.util.LocalFSOperationsUtil;
import com.ikaver.aagarwal.hw3.common.util.SocketAddress;
import com.ikaver.aagarwal.hw3.common.workers.IReducer;
import com.ikaver.aagarwal.hw3.common.workers.MapWorkDescription;
import com.ikaver.aagarwal.hw3.common.workers.ReduceWorkDescription;
import com.ikaver.aagarwal.hw3.common.workers.WorkerState;

/**
 * Runs a reduce job for a given reduce work description.
 */
public class MRReduceRunner implements Runnable {

	private static final Logger LOGGER = Logger.getLogger(MRReduceRunner.class);

	private final ReduceWorkDescription rwd;
	private final Map<String, List<String>> aggregator;
	private final SocketAddress masterAddress;

	private WorkerState state;

	public MRReduceRunner(ReduceWorkDescription rwd, SocketAddress masterAddress) {
		this.rwd = rwd;
		this.setState(WorkerState.RUNNING);
		this.aggregator = new TreeMap<String, List<String>>();
		this.masterAddress = masterAddress;
	}

	public void run() {
	  LOGGER.info("Starting reducer work...");

		List<MapWorkDescription> mwds = rwd.getMappers();
		List<SocketAddress> mapperAddresses = rwd.getMapperAddresses();

		for (int i = 0; i < mwds.size(); i++) {
			IMRNodeManager node = NodeManagerFactory
					.nodeManagerFromSocketAddress(mapperAddresses.get(i));
			if (node == null) {
				setState(WorkerState.FAILED);
				LOGGER.warn("error fetching data from node manager");
				return;
			} else {
				List<KeyValuePair> list;
				try {
					list = node.dataForJob(mwds.get(i), rwd);
					aggregate(list); //aggregate all lists
				} catch (RemoteException e) {
					setState(WorkerState.FAILED);
					LOGGER.warn("Encountered remote exception while"
							+ "trying to fetch data from a node.");
					return;
				}
			}
		}

		IReducer reducer = getReducerClass(rwd);
		if (reducer == null) {
			LOGGER.warn("Unable to fetch class from the jar file.");
			setState(WorkerState.FAILED);
			return;
		}
		IMRReducerCollector collector = new MRReducerCollector();

		for (String key : aggregator.keySet()) {
			try {
				reducer.reduce(collector, key, aggregator.get(key));
			} catch (Exception e) {
			  LOGGER.warn("Reducer failed! ", e);
				setState(WorkerState.FAILED);
				return;
			}
		}

		IDFS dfs = DFSFactory.dfsFromSocketAddress(masterAddress);

		if (dfs == null) {
			LOGGER.error("Error communicating with the master node.");
			setState(WorkerState.FAILED);
			return;
		}
		byte[] data = collector.getData();
		LOGGER.info("Size of data (in bytes) for reducer " + rwd.getReducerID()
				+ "is " + data.length);

		try {
		  LOGGER.info("Saving reducer file on DFS...");
			dfs.createFile(getReducerPartitionName(rwd), data.length,
					data.length);
			dfs.saveFile(getReducerPartitionName(rwd), 0, data);
			LOGGER.info("Saved file on DFS. Finished.");
			setState(WorkerState.FINISHED);
			return;
		} catch (RemoteException e) {
			setState(WorkerState.FAILED);
			LOGGER.error("Error saving reducer output to a file", e);
			return;
		}

	}

	private void aggregate(List<KeyValuePair> kvs) {
		if (kvs == null) {
			return;
		}
		for (KeyValuePair kv : kvs) {
			if (aggregator.get(kv.getKey()) == null) {
				aggregator.put(kv.getKey(), new ArrayList<String>());
			}
			aggregator.get(kv.getKey()).add(kv.getValue());
		}
	}

	private IReducer getReducerClass(ReduceWorkDescription input) {
		IReducer reducer = null;
		String jarPath = LocalFSOperationsUtil.storeLocalFile(input.getJarFile(),
				".jar");
		try {
			File file = new File(jarPath);
			ClassLoader loader = new URLClassLoader(new URL[] { file.toURI()
					.toURL() });
			Class<IReducer> reducerClass = (Class<IReducer>) loader
					.loadClass(input.getReducerClass());
			reducer = reducerClass.newInstance();
		} catch (IOException e) {
			LOGGER.fatal("Error reading jar file from the disk. Either the"
					+ "file" + jarPath
					+ "doesn't exist on the local filesystem"
					+ " or the local disk is full");
		} catch (ClassNotFoundException e) {
			LOGGER.fatal("Unable to locate the reducer class"
					+ input.getReducerClass());
		} catch (InstantiationException e) {
			LOGGER.fatal("Error instanting the reducer class"
					+ input.getReducerClass());
		} catch (IllegalAccessException e) {
		}
		return reducer;
	}

	private String getReducerPartitionName(ReduceWorkDescription rwd) {
		return String.format("%s-%d.out", rwd.getOutputFilePath(),
				rwd.getReducerID());
	}

	public WorkerState getReducerState() throws RemoteException {
		return getState();
	}

	public WorkerState getState() {
		return state;
	}

	private synchronized void setState(WorkerState state) {
		this.state = state;
	}

}
