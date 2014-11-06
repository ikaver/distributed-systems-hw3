package com.ikaver.aagarwal.hw3.common.dfs;

public interface IDataNode {
  
  public byte [] getFile(String filePath);
  public boolean saveFile(String filePath, byte [] file);

}
