package com.ikaver.aagarwal.hw3.mrnodemanager.runner;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;

import com.ikaver.aagarwal.hw3.common.util.FileOperationsUtil;
import com.ikaver.aagarwal.hw3.common.util.SocketUtil;
import com.ikaver.aagarwal.hw3.common.workers.MapWorkDescription;

/**
 * Forks a new instance of a map task and has all the necessary information
 * to communicate with it.
 */
public class MRMapTaskAttempt {

	private static final Logger LOGGER = Logger.getLogger(MRMapTaskAttempt.class);
	private static final String MR_MAP_ATTEMPT = "mr-map-attempt";
	/**
	 * Returns the port at which the remote object for task attempt
	 * is bound.
	 */
	public static int startMapTask(MapWorkDescription input) {
		int port = SocketUtil.findFreePort();
		
		// TODO(ankit): Remove this!
		ProcessBuilder builder = new ProcessBuilder("java", "-jar"
				,"mrmap-1.0-SNAPSHOT-jar-with-dependencies.jar", "-port", port + "",
				">", getStdoutRedirectionFile(input), "2>", getStderrRedirectionFile(input));

		try {
			Process process = builder.start();
		} catch (IOException e) {
			LOGGER.fatal("Error starting map task attempt at port: " + port);
			e.printStackTrace();
			return -1;
		}
		return port;
	}
	
	private static String getStdoutRedirectionFile(MapWorkDescription input) {
		return FileOperationsUtil.getRandomStringForLocalFile()
				+ "_" + MR_MAP_ATTEMPT + "__job_id__" + input.getJobID() + 
				"__partition__" + input.getChunk() + ".stdout";
	}
	
	private static String getStderrRedirectionFile(MapWorkDescription input) {
		return FileOperationsUtil.getRandomStringForLocalFile()
				+ "_" + MR_MAP_ATTEMPT + "__job_id__" + input.getJobID() + 
				"__partition__" + input.getChunk() + ".stderr";
	}

}
