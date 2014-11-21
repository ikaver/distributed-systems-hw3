package com.ikaver.aagarwal.hw3.mrnodemanager.runner;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.ikaver.aagarwal.hw3.common.config.MRConfig;
import com.ikaver.aagarwal.hw3.common.util.SocketUtil;

/**
 * Forks a new instance of a map task and has all the necessary information to
 * communicate with it.
 */
public class MRReduceTaskAttempt {

	private static final Logger LOGGER = Logger
			.getLogger(MRReduceTaskAttempt.class);

	/**
	 * Returns the port at which the remote object for task attempt is bound.
	 */
	public static int startReduceTask(String masterIP, int masterPort) {
		int port = SocketUtil.findFreePort();

		try {
			Runtime.getRuntime().exec(
					"java -jar mrreduce-1.0-SNAPSHOT-jar-with-dependencies.jar "
							+ " -port " + port
							+ " -config " + MRConfig.getConfigFileName() );
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				LOGGER.warn("Interrupted", e);
			}
		} catch (IOException e) {
			LOGGER.fatal("Error starting reduce task attempt at port: " + port, e);
			return -1;
		}
		return port;
	}
}
