package com.ikaver.aagarwal.hw3.mrdfs.master;

import com.ikaver.aagarwal.hw3.common.util.SocketAddress;

public interface IOnDataNodeFailureHandler {
  
  public void onDataNodeFailed(SocketAddress addr);

}
