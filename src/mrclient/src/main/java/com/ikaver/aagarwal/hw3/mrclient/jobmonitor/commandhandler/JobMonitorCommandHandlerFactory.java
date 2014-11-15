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

public class JobMonitorCommandHandlerFactory implements ICommandHandlerFactory {

  private static final String LIST_JOBS = "list";
  private static final String CREATE_JOB = "create";
  private static final String JOB_INFO = "info";
  private static final String TERMINATE_JOB = "terminate";
  private static final String UPLOAD_FILE = "upload";

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
    commandHandlers.put(UPLOAD_FILE, new UploadFileCommandHandler(masterSocketAddress));
    return commandHandlers;
  }

}
