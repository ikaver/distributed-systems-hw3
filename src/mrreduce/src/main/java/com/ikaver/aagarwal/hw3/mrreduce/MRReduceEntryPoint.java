package com.ikaver.aagarwal.hw3.mrreduce;

import java.io.IOException;
import java.rmi.Naming;
import java.rmi.registry.LocateRegistry;

import org.apache.log4j.FileAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.ikaver.aagarwal.hw3.common.config.MRConfig;
import com.ikaver.aagarwal.hw3.common.definitions.Definitions;
import com.ikaver.aagarwal.hw3.common.dfs.FileUtil;
import com.ikaver.aagarwal.hw3.common.mrreduce.IMRReduceInstanceRunner;
import com.ikaver.aagarwal.hw3.common.util.LocalFSOperationsUtil;
import com.ikaver.aagarwal.hw3.common.util.SocketAddress;
import com.ikaver.aagarwal.hw3.common.workers.flags.MRWorkerRunnerSettings;

/**
 * Entry point for an instance which runs a "reduce" instance.
 */
public class MRReduceEntryPoint {

	private static final Logger LOG = Logger
			.getLogger(MRReduceEntryPoint.class);

	public static void main(String args[]) throws IOException {
	  //Read command line args
		MRWorkerRunnerSettings settings = new MRWorkerRunnerSettings();
    JCommander cmd = new JCommander(settings);
    String configFilePath = null;
    try {
      cmd.parse(args);
      configFilePath = settings.getConfigFilePath();
    } catch (ParameterException ex) {
      cmd.usage();
      System.exit(-1);
    }

    //Setup config parameters
    if(!MRConfig.setupFromConfigFile(configFilePath)) {
      LOG.error("Failed to read setup file.");
      System.exit(-1);
    }
		
    //Create log file
		FileAppender appender = new FileAppender(new PatternLayout(PatternLayout.DEFAULT_CONVERSION_PATTERN),
				getLogFileForPort(settings.getPort()));
		LocalFSOperationsUtil.changeFilePermission(getLogFileForPort(settings.getPort()));
		Logger.getRootLogger().addAppender(appender);

		
    //Create reduce instance runner, setup RMI service
		SocketAddress masterAddress = MRConfig.getMasterSocketAddress();
		Injector injector = Guice.createInjector(new MRReduceModule(masterAddress));
		IMRReduceInstanceRunner runner =
				injector.getInstance(IMRReduceInstanceRunner.class);
		LocateRegistry.createRegistry(settings.getPort());
		Naming.rebind(String.format("//:%d/"
				+ Definitions.MR_REDUCE_RUNNER_SERVICE, settings.getPort()),
				runner);
		
		LOG.info("Logger is now running on port" + settings.getPort());
	}
	
	public static String getLogFileForPort(int port) {
		return "log.reducer." + port;
	}

}
