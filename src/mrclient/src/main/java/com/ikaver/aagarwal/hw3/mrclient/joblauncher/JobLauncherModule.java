package com.ikaver.aagarwal.hw3.mrclient.joblauncher;

import com.google.inject.AbstractModule;

public class JobLauncherModule extends AbstractModule {
  
  @Override
  protected void configure() {
    bind(IJobManagerFactory.class).to(JobManagerFactoryImpl.class);
    /*
    bind(InputStream.class).annotatedWith(Names.named("ControllerInput"))
      .toInstance(System.in);
    ReadWriteLock stateLock = new ReentrantReadWriteLock();
    SubscribedProcessRunnersState state = new SubscribedProcessRunnersState();
    IProcessRunnerFactory factory = new ProcessRunnerFactoryImpl();
    
    bind(ReadWriteLock.class).annotatedWith(Names.named("NMStateLock"))
      .toInstance(stateLock);
    bind(SubscribedProcessRunnersState.class).annotatedWith(Names.named("NMState"))
      .toInstance(state);
    bind(IProcessRunnerFactory.class).annotatedWith(Names.named("ProcessManagerFactory"))
      .toInstance(factory);
    */
  }
}
