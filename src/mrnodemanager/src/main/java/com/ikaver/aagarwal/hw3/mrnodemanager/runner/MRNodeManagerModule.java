package com.ikaver.aagarwal.hw3.mrnodemanager.runner;

import com.google.inject.AbstractModule;
import com.ikaver.aagarwal.hw3.common.dfs.IDataNode;
import com.ikaver.aagarwal.hw3.common.nodemanager.IMRNodeManager;
import com.ikaver.aagarwal.hw3.mrdfs.datanode.DataNodeImpl;

public class MRNodeManagerModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(IMRNodeManager.class).to(MRNodeManager.class);
    bind(IDataNode.class).to(DataNodeImpl.class);
  }
  
  

}
