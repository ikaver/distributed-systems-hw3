package com.ikaver.aagarwal.hw3.mrdfs.datanode;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.rmi.RemoteException;

import com.ikaver.aagarwal.hw3.common.definitions.Definitions;
import com.ikaver.aagarwal.hw3.common.dfs.IDataNode;

public class DataNodeImpl implements IDataNode {

  public byte[] getFile(String filePath) throws IOException {
    File file = new File(Definitions.BASE_DIRECTORY + filePath);
    FileInputStream fis = new FileInputStream(file);
    byte[] data = new byte[(int)file.length()];
    fis.read(data);
    fis.close();
    return data;
  }

  public void saveFile(String filePath, byte[] data) throws IOException {
    File file = new File(Definitions.BASE_DIRECTORY + filePath);
    FileOutputStream fos = new FileOutputStream(file);
    fos.write(data);
    fos.close();
  }

  public long sizeOfFileInBytes(String filePath) throws IOException,
      RemoteException {
    File file = new File(Definitions.BASE_DIRECTORY + filePath);
    return file.length();
  }

  public boolean createDirectory(String path) throws RemoteException {
    throw new UnsupportedOperationException("Not yet implemented");
  }

}
