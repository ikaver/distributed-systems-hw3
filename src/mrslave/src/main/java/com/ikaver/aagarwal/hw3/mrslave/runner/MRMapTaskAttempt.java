package com.ikaver.aagarwal.hw3.mrslave.runner;

import java.io.IOException;
import java.util.Random;

import org.apache.log4j.Logger;

import com.ikaver.aagarwal.hw3.common.config.MRMapTaskInput;

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
	public static int startMapTask(MRMapTaskInput input) {

		Random random = new Random();
		random.setSeed(System.currentTimeMillis());

		int port = random.nextInt();

		// TODO(ankit): Remove this!
		ProcessBuilder builder = new ProcessBuilder("java -jar "
				+ "/tmp/mrmap-1.0-SNAPSHOT-jar-with-dependencies.jar -port " + random.nextInt());

		try {
			builder.start();
		} catch (IOException e) {
			LOGGER.fatal("Error starting map task attempt at port: " + port);
			return -1;
		}
		return port;
	}
}
