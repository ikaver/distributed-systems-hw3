package com.ikaver.aagarwal.hw3.common.dfs;

import java.util.Set;

import com.ikaver.aagarwal.hw3.common.util.SocketAddress;

public interface IDFS {
  
  public Set<SocketAddress> dataNodeForFile(String filePath);
  public boolean saveFile(String filePath, byte [] file);
  
}
