package com.ikaver.aagarwal.hw3.mrnodemanager.runner;

import java.util.HashMap;
import java.util.Map;

import com.google.inject.AbstractModule;
import com.google.inject.name.Names;
import com.ikaver.aagarwal.hw3.common.definitions.Definitions;
import com.ikaver.aagarwal.hw3.common.dfs.IDataNode;
import com.ikaver.aagarwal.hw3.common.nodemanager.IMRNodeManager;
import com.ikaver.aagarwal.hw3.common.util.SocketAddress;
import com.ikaver.aagarwal.hw3.mrdfs.datanode.DataNodeImpl;

public class MRNodeManagerModule extends AbstractModule {

	private SocketAddress masterAddr;

	public MRNodeManagerModule(SocketAddress masterAddr) {
		this.masterAddr = masterAddr;
	}

	@Override
	protected void configure() {
		bind(SocketAddress.class).annotatedWith(
				Names.named(Definitions.MASTER_SOCKET_ADDR_ANNOTATION))
				.toInstance(this.masterAddr);

		bind(IMRNodeManager.class).to(MRNodeManagerImpl.class);
		bind(IDataNode.class).to(DataNodeImpl.class);
	}
}
