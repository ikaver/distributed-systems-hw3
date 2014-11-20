package com.ikaver.aagarwal.hw3.mrmap;

import java.io.IOException;
import java.rmi.Naming;
import java.rmi.registry.LocateRegistry;

import org.apache.log4j.FileAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import com.beust.jcommander.JCommander;
import com.ikaver.aagarwal.hw3.common.definitions.Definitions;
import com.ikaver.aagarwal.hw3.common.util.SocketAddress;
import com.ikaver.aagarwal.hw3.common.workers.flags.MRWorkerRunnerSettings;

public class MapRunnerEntryPoint {
	
	private static final Logger LOGGER = Logger.getLogger(MapRunnerEntryPoint.class);

	public static void main(String args[]) throws IOException {
		MRWorkerRunnerSettings settings = new MRWorkerRunnerSettings();
		JCommander cmd = new JCommander(settings);
		cmd.parse(args);

		FileAppender appender = new FileAppender(new PatternLayout(PatternLayout.DEFAULT_CONVERSION_PATTERN),
				"log.mapper." + settings.getPort());
		Logger.getRootLogger().addAppender(appender);

		// TODO(ankit): Guicify it.
		SocketAddress masterAddress = new SocketAddress(settings.getMasterIP(),
				settings.getMasterPort());

		MapInstanceRunner runner = new MapInstanceRunner(masterAddress);

		LocateRegistry.createRegistry(settings.getPort());

		Naming.rebind(
				String.format("//:%d/" + Definitions.MR_MAP_RUNNER_SERVICE,
						settings.getPort()), runner);

		LOGGER.info("Logger is now running on port" + settings.getPort());
	}
}