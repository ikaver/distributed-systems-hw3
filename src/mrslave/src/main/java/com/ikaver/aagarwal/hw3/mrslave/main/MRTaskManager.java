package com.ikaver.aagarwal.hw3.mrslave.main;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.rmi.RemoteException;

import org.apache.log4j.Logger;

import com.ikaver.aagarwal.hw3.common.config.MRMapTaskInput;
import com.ikaver.aagarwal.hw3.common.slave.IMRTaskManager;
import com.ikaver.aagarwal.hw3.common.workers.IMapper;

/**
 * A task manager manages a "slave" node. Following are the responsibilities of
 * a task manager.
 * 1. Accepts a jar and forks out separate jvms for running mapper and reducer
 *   tasks.
 * 2. Periodically updates master with the status of the map reduce
 *   job assigned to it.
 */
public class MRTaskManager implements IMRTaskManager {

	private static final Logger LOGGER = Logger.getLogger(MRTaskManager.class);

	// TODO(ankit): Return a failure error code.
	@SuppressWarnings("resource")
	public void doMap(MRMapTaskInput input) throws RemoteException {

		// TODO(ankit): Move this code inside the child jvm.
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
		} catch (IllegalAccessException e) {
		}
	}

	public void doReduce() throws RemoteException {
		// TODO Auto-generated method stub
		
	}
}
