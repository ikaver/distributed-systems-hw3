package com.ikaver.aagarwal.hw3.common.dfs;

import java.io.IOException;

public interface IDataNode {
  
  public byte [] getFile(String filePath) throws IOException;
  public void saveFile(String filePath, byte [] file) throws IOException;

}
