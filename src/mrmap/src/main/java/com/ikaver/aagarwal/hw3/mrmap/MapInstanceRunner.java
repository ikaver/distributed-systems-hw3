package com.ikaver.aagarwal.hw3.mrmap;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

import org.apache.log4j.Logger;

import com.ikaver.aagarwal.hw3.common.mrmap.IMapInstanceRunner;
import com.ikaver.aagarwal.hw3.common.workers.IMapper;
import com.ikaver.aagarwal.hw3.common.workers.MapWorkDescription;
import com.ikaver.aagarwal.hw3.common.workers.WorkerState;


public class MapInstanceRunner extends UnicastRemoteObject
    implements IMapInstanceRunner {

	private static final long serialVersionUID = -2794021388396287951L;

	private static final Logger LOGGER = Logger.getLogger(MapInstanceRunner.class);

	protected MapInstanceRunner() throws RemoteException {
		super();
	}

	public void runMapInstance(MapWorkDescription input) {
		try {
	  		  File file = new File(input.getJarPath());
	  		  ClassLoader loader = new URLClassLoader(new URL[] {file.toURI().toURL()});
	  		  Class<IMapper> mapperClass = (Class<IMapper>) loader.loadClass(input.getMapperClass());
	  		  IMapper mapper = mapperClass.newInstance();
	  		  mapper.map();
			} catch (IOException e) {
				LOGGER.fatal("Error reading jar file from the disk. Either the" +
				    "file" + input.getJarPath() + "doesn't exist on the local filesystem" +
				    " or the local disk is full");
			} catch (ClassNotFoundException e) {
				LOGGER.fatal("Unable to locate the mapper class" + input.getMapperClass());
			} catch (InstantiationException e) {
				LOGGER.fatal("Error instanting the mapper class" + input.getMapperClass());
			} catch (IllegalAccessException e) {
			}
	}
	
	/**
	 * Die in peace JVM. You have done your duty.
	 */
	public void die() {
		System.exit(1);
	}

	public WorkerState getMapperState() {
		return WorkerState.FINISHED;
	}

	public String getMapOutputFilePath() {
		return null;
	}
}
