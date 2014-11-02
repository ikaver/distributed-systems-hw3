package com.ikaver.aagarwal.hw3.mrclient.jobmonitor;

import com.google.inject.AbstractModule;
import com.google.inject.name.Names;
import com.ikaver.aagarwal.hw3.common.commandhandler.ICommandHandlerFactory;
import com.ikaver.aagarwal.hw3.common.definitions.Definitions;
import com.ikaver.aagarwal.hw3.mrclient.jobmonitor.commandhandler.JobMonitorCommandHandlerFactory;

public class JobMonitorModule extends AbstractModule {
  
  private String masterIP;
  private int masterPort;
  
  public JobMonitorModule(String masterIP, int masterPort) {
    this.masterIP = masterIP;
    this.masterPort = masterPort;
  }

  @Override
  protected void configure() {
    bind(String.class).annotatedWith(Names.named(Definitions.MASTER_IP_ANNOTATION))
      .toInstance(this.masterIP);
    bind(Integer.class).annotatedWith(Names.named(Definitions.MASTER_PORT_ANNOTATION))
    .toInstance(this.masterPort);
  
    bind(ICommandHandlerFactory.class).to(JobMonitorCommandHandlerFactory.class);
    bind(IJobManagerFactory.class).to(JobManagerFactoryImpl.class);
  }
  
  

}
