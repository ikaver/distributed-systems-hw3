package com.ikaver.aagarwal.hw3.mrclient.jobmonitor.commandhandler;

import java.util.HashMap;
import java.util.Map;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.ikaver.aagarwal.hw3.common.commandhandler.ICommandHandler;
import com.ikaver.aagarwal.hw3.common.commandhandler.ICommandHandlerFactory;
import com.ikaver.aagarwal.hw3.common.definitions.Definitions;
import com.ikaver.aagarwal.hw3.common.util.SocketAddress;
import com.ikaver.aagarwal.hw3.mrclient.jobmonitor.JobMonitor;

/**
 * Creates all of the command listeners for the client project.
 */
public class JobMonitorCommandHandlerFactory implements ICommandHandlerFactory {

  private static final String LIST_JOBS = "list";
  private static final String CREATE_JOB = "create";
  private static final String JOB_INFO = "info";
  private static final String TERMINATE_JOB = "terminate";
  private static final String UPLOAD_FILE = "upload";
  private static final String DOWNLOAD_FILE = "download";
  private static final String LIST_FINISHED_JOBS = "finished";
  private static final String SHUTDOWN = "shutdown";
  private static final String END_SESSION = "end-session";

  private JobMonitor monitor;
  private SocketAddress masterSocketAddress;

  @Inject
  public JobMonitorCommandHandlerFactory(JobMonitor monitor, 
      @Named(Definitions.MASTER_SOCKET_ADDR_ANNOTATION) SocketAddress masterAddr) {
    this.monitor = monitor;
    this.masterSocketAddress = masterAddr;
  }

  public Map<String, ICommandHandler> getCommandHandlers() {
    Map<String, ICommandHandler> commandHandlers = new HashMap<String, ICommandHandler>();
    commandHandlers.put(LIST_JOBS, new ListJobsCommandHandler(monitor));
    commandHandlers.put(CREATE_JOB, new CreateJobCommandHandler(monitor));
    commandHandlers.put(JOB_INFO, new JobInfoCommandHandler(monitor));
    commandHandlers.put(TERMINATE_JOB, new TerminateJobCommandHandler(monitor));
    commandHandlers.put(LIST_FINISHED_JOBS, new ListFinishedJobsCommandHandler(monitor));
    commandHandlers.put(UPLOAD_FILE, new UploadFileCommandHandler(masterSocketAddress));
    commandHandlers.put(DOWNLOAD_FILE, new DownloadFileCommandHandler(masterSocketAddress));
    commandHandlers.put(SHUTDOWN, new ShutdownCommandHandler(monitor));
    commandHandlers.put(END_SESSION, new EndClientSystemCommandHandler());
    return commandHandlers;
  }

}
