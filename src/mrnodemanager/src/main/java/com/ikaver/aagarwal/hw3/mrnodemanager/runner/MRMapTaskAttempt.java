package com.ikaver.aagarwal.hw3.mrnodemanager.runner;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.ikaver.aagarwal.hw3.common.util.SocketUtil;
import com.ikaver.aagarwal.hw3.common.workers.MapWorkDescription;

/**
 * Forks a new instance of a map task and has all the necessary information
 * to communicate with it.
 */
public class MRMapTaskAttempt {

	private static final Logger LOGGER = Logger.getLogger(MRMapTaskAttempt.class);
	/**
	 * Returns the port at which the remote object for task attempt
	 * is bound.
	 */
	public static int startMapTask(MapWorkDescription input) {

		int port = SocketUtil.findFreePort();

		// TODO(ankit): Remove this!
		ProcessBuilder builder = new ProcessBuilder("java", "-jar"
				,"mrmap-1.0-SNAPSHOT-jar-with-dependencies.jar", "-port", port + "");

		try {
			builder.start();
		} catch (IOException e) {
			LOGGER.fatal("Error starting map task attempt at port: " + port);
			e.printStackTrace();
			return -1;
		}
		return port;
	}
}
