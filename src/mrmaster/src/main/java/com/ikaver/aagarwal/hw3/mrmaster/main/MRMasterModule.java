package com.ikaver.aagarwal.hw3.mrmaster.main;

import com.google.inject.AbstractModule;
import com.ikaver.aagarwal.hw3.common.master.IJobManager;
import com.ikaver.aagarwal.hw3.mrmaster.jobmanager.JobManagerMockImpl;

public class MRMasterModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(IJobManager.class).to(JobManagerMockImpl.class);
  }

}
