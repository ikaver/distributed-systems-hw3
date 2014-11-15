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
import com.ikaver.aagarwal.hw3.common.workers.IMapper;
import com.ikaver.aagarwal.hw3.common.workers.MapWorkDescription;
import com.ikaver.aagarwal.hw3.common.workers.WorkerState;

public class MapInstanceRunner extends UnicastRemoteObject implements
		IMapInstanceRunner {

	private static final long serialVersionUID = -2794021388396287951L;
	private static final Logger LOGGER = Logger
			.getLogger(MapInstanceRunner.class);

	private final MapOutputCollector moc;

	private String mocFilePath;

	protected MapInstanceRunner() throws RemoteException {
		super();
		moc = new MapOutputCollector();
	}

	/**
	 * Somehow need to know the name of the input file on which this mapper
	 * needs to act.
	 */
	public void runMapInstance(MapWorkDescription input, String localFilePath) {
		LOGGER.info("Recived " + input.getJobID() + " " + input.getChunk().getInputFilePath() + " " + localFilePath);
		IMapper mapper = getMapperClass(input);

		FileInputStream fis;

		try {
			fis = new FileInputStream(new File(localFilePath));
		} catch (FileNotFoundException e) {
			LOGGER.fatal("Mapper was expecting" + localFilePath
					+ " to be accessible.");
			return;
		}

		byte[] record = new byte[input.getChunk().getRecordSize()];
		try {
			while (fis.read(record) != -1) {
				mapper.map(record.toString(), (ICollector)moc);
			}

			// Flush the data to a file and return the path where it is
			// being stored.
			this.mocFilePath = moc.flush();

		} catch (IOException e) {
			LOGGER.fatal("error reading data from the local file system. Check"
					+ "that ");
		}
	}

	/**
	 * Die in peace JVM. You have done your duty.
	 */
	public void die() {
		System.exit(1);
	}

	public WorkerState getMapperState() {
		throw new UnsupportedOperationException("Not yet implemented :(");
	}

	public String getMapOutputFilePath() {
		return mocFilePath;
	}
	
	private IMapper getMapperClass(MapWorkDescription input) {
		IMapper mapper = null;


		return mapper;
	}
}
