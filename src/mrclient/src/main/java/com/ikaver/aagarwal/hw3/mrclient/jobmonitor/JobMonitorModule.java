package com.ikaver.aagarwal.hw3.mrclient.jobmonitor;

import com.google.inject.AbstractModule;
import com.google.inject.name.Names;
import com.ikaver.aagarwal.hw3.common.commandhandler.ICommandHandlerFactory;
import com.ikaver.aagarwal.hw3.common.definitions.Definitions;
import com.ikaver.aagarwal.hw3.common.util.SocketAddress;
import com.ikaver.aagarwal.hw3.mrclient.jobmonitor.commandhandler.JobMonitorCommandHandlerFactory;

public class JobMonitorModule extends AbstractModule {
  
  private SocketAddress masterAddr;
  
  public JobMonitorModule(SocketAddress masterAddr) {
    this.masterAddr = masterAddr;
  }

  @Override
  protected void configure() {
    bind(SocketAddress.class).annotatedWith(Names.named(Definitions.MASTER_SOCKET_ADDR_ANNOTATION))
      .toInstance(this.masterAddr);
  
    bind(ICommandHandlerFactory.class).to(JobMonitorCommandHandlerFactory.class);
    bind(IJobManagerFactory.class).to(JobManagerFactoryImpl.class);
  }
  
  

}
