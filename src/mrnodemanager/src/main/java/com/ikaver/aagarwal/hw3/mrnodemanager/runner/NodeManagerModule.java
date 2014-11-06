package com.ikaver.aagarwal.hw3.mrnodemanager.runner;

import com.google.inject.AbstractModule;
import com.ikaver.aagarwal.hw3.common.nodemanager.IMRNodeManager;

public class NodeManagerModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(IMRNodeManager.class).to(MRNodeManager.class);
  }
  
  

}
