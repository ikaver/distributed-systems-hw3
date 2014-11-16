package com.ikaver.aagarwal.hw3.mrmap;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

import org.apache.log4j.Logger;

import com.ikaver.aagarwal.hw3.common.mrmap.ICollector;
import com.ikaver.aagarwal.hw3.common.mrmap.IMapInstanceRunner;
import com.ikaver.aagarwal.hw3.common.util.FileOperationsUtil;
import com.ikaver.aagarwal.hw3.common.workers.IMapper;
import com.ikaver.aagarwal.hw3.common.workers.MapWorkDescription;
import com.ikaver.aagarwal.hw3.common.workers.WorkerState;

public class MapInstanceRunner extends UnicastRemoteObject implements
		IMapInstanceRunner {

	private static final long serialVersionUID = -2794021388396287951L;
	private static final Logger LOGGER = Logger
			.getLogger(MapInstanceRunner.class);

	private final MapOutputCollector moc;

	private final MapWorkState mapWorkState;

	protected MapInstanceRunner() throws RemoteException {
		super();
		mapWorkState = new MapWorkState();
		moc = new MapOutputCollector();
	}

	/**
	 * Somehow need to know the name of the input file on which this mapper
	 * needs to act.
	 */
	public void runMapInstance(MapWorkDescription input, String localFilePath) {
		LOGGER.info("Received " + input.getJobID() + " " + input.getChunk().getInputFilePath() + " " + localFilePath);
		IMapper mapper = getMapperClass(input);

		FileInputStream fis;

		try {
			fis = new FileInputStream(new File(localFilePath));
		} catch (FileNotFoundException e) {
			LOGGER.fatal("Mapper was expecting" + localFilePath
					+ " to be accessible.");
			mapWorkState.setState(WorkerState.FAILED);
			return;
		}

		mapWorkState.setState(WorkerState.RUNNING);

		byte[] record = new byte[input.getChunk().getRecordSize()];
		try {
			while (fis.read(record) != -1) {
				mapper.map(record.toString(), (ICollector)moc);
			}

			// Set the output path before you set the state. Otherwise,
			// it can lead to a subtle concurrency bug where you try to
			// access the path which has not yet been set while the map
			// state has been set.
			mapWorkState.setOutputPath(moc.flush());

			// Flush the data to a file and return the path where it is
			// being stored.
			if (mapWorkState.getOutputPath() != null) {
				mapWorkState.setState(WorkerState.FAILED);
			} else {
				mapWorkState.setState(WorkerState.FINISHED);
			}

		} catch (IOException e) {
			mapWorkState.setState(WorkerState.FAILED);
			LOGGER.fatal("error reading data from the local file system. Check"
					+ "that the file is accessible.");
		}
	}

	/**
	 * Die in peace JVM. You have done your duty.
	 */
	public void die() {
		System.exit(1);
	}

	public WorkerState getMapperState() {
		return mapWorkState.getState();
	}

	public String getMapOutputFilePath() {
		return mapWorkState.getOutputPath();
	}
	
	private IMapper getMapperClass(MapWorkDescription input) {
		IMapper mapper = null;
		String jarPath = FileOperationsUtil.storeLocalFile(input.getJarFile(), ".jar");
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
}
