package com.ikaver.aagarwal.hw3.mrmap;

import java.io.IOException;
import java.rmi.Naming;
import java.rmi.registry.LocateRegistry;

import org.apache.log4j.FileAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.ikaver.aagarwal.hw3.common.config.MRConfig;
import com.ikaver.aagarwal.hw3.common.definitions.Definitions;
import com.ikaver.aagarwal.hw3.common.util.LocalFSOperationsUtil;
import com.ikaver.aagarwal.hw3.common.util.SocketAddress;
import com.ikaver.aagarwal.hw3.common.workers.flags.MRWorkerRunnerSettings;

public class MapRunnerEntryPoint {
	
	private static final Logger LOG = Logger.getLogger(MapRunnerEntryPoint.class);

	public static void main(String args[]) throws IOException {
		//Parse command line args
	  MRWorkerRunnerSettings settings = new MRWorkerRunnerSettings();
    JCommander argsParser = new JCommander(settings);
    String configFilePath = null;
    try {
      argsParser.parse(args);
      configFilePath = settings.getConfigFilePath();
    } catch (ParameterException ex) {
      argsParser.usage();
      System.exit(-1);
    }

    //Setup config parameters
    if(!MRConfig.setupFromConfigFile(configFilePath)) {
      LOG.error("Failed to read setup file.");
      System.exit(-1);
    }

		//Create log file
		FileAppender appender = new FileAppender(
		    new PatternLayout(PatternLayout.DEFAULT_CONVERSION_PATTERN),
				logFileForPort(settings.getPort()));
		Logger.getRootLogger().addAppender(appender);
		LocalFSOperationsUtil.changeFilePermission(logFileForPort(settings.getPort()));

		//Create map instance runner, setup RMI service
    SocketAddress masterAddress = MRConfig.getMasterSocketAddress();
		MapInstanceRunner runner = new MapInstanceRunner(masterAddress);
		LocateRegistry.createRegistry(settings.getPort());
		Naming.rebind(
				String.format("//:%d/" + Definitions.MR_MAP_RUNNER_SERVICE,
						settings.getPort()), runner);

		LOG.info("Logger is now running on port" + settings.getPort());
	}

	private static String logFileForPort(int port) {
		return "log.mapper." + port;
	}
}