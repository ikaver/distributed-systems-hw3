package com.ikaver.aagarwal.hw3.mrrreduce;

import com.google.inject.AbstractModule;
import com.google.inject.name.Names;
import com.ikaver.aagarwal.hw3.common.definitions.Definitions;
import com.ikaver.aagarwal.hw3.common.mrreduce.IMRReduceInstanceRunner;
import com.ikaver.aagarwal.hw3.common.util.SocketAddress;

public class MRReduceModule extends AbstractModule {

	private SocketAddress masterAddr;

	public MRReduceModule(SocketAddress masterAddr) {
		this.masterAddr = masterAddr;
	}

	@Override
	protected void configure() {
		bind(SocketAddress.class).annotatedWith(
				Names.named(Definitions.MASTER_SOCKET_ADDR_ANNOTATION))
				.toInstance(this.masterAddr);
		bind(IMRReduceInstanceRunner.class).to(
				MRReduceInstanceRunner.class);
	}
}
