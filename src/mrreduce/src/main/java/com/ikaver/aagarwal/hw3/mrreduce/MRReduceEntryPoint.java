package com.ikaver.aagarwal.hw3.mrreduce;

import java.io.IOException;
import java.io.ObjectInputStream.GetField;
import java.rmi.Naming;
import java.rmi.registry.LocateRegistry;

import org.apache.log4j.FileAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import com.beust.jcommander.JCommander;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.ikaver.aagarwal.hw3.common.definitions.Definitions;
import com.ikaver.aagarwal.hw3.common.dfs.FileUtil;
import com.ikaver.aagarwal.hw3.common.mrreduce.IMRReduceInstanceRunner;
import com.ikaver.aagarwal.hw3.common.util.SocketAddress;
import com.ikaver.aagarwal.hw3.common.workers.flags.MRWorkerRunnerSettings;

/**
 * Entry point for an instance which runs a "reduce" instance.
 */
public class MRReduceEntryPoint {

	private static final Logger LOGGER = Logger
			.getLogger(MRReduceEntryPoint.class);

	public static void main(String args[]) throws IOException {

		MRWorkerRunnerSettings settings = new MRWorkerRunnerSettings();

		JCommander cmd = new JCommander(settings);
		cmd.parse(args);
		
		FileAppender appender = new FileAppender(new PatternLayout(PatternLayout.DEFAULT_CONVERSION_PATTERN),
				getLogFileForPort(settings.getPort()));

		FileUtil.changeFilePermission(getLogFileForPort(settings.getPort()));

		Logger.getRootLogger().addAppender(appender);

		SocketAddress masterAddress = new SocketAddress(settings.getMasterIP(),
				settings.getMasterPort());

		Injector injector = Guice.createInjector(new MRReduceModule(masterAddress));

		IMRReduceInstanceRunner runner =
				injector.getInstance(IMRReduceInstanceRunner.class);

		LocateRegistry.createRegistry(settings.getPort());

		Naming.rebind(String.format("//:%d/"
				+ Definitions.MR_REDUCE_RUNNER_SERVICE, settings.getPort()),
				runner);

		LOGGER.info("Logger is now running on port" + settings.getPort());

	}
	
	public static String getLogFileForPort(int port) {
		return "log.reducer." + port;
	}

}
