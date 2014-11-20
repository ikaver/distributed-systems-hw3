package com.ikaver.aagarwal.hw3.mrclient.dfs;

import java.io.FileOutputStream;
import java.io.IOException;
import java.rmi.RemoteException;

import org.apache.log4j.Logger;

import com.ikaver.aagarwal.hw3.common.dfs.DFSFactory;
import com.ikaver.aagarwal.hw3.common.dfs.FileMetadata;
import com.ikaver.aagarwal.hw3.common.dfs.IDFS;
import com.ikaver.aagarwal.hw3.common.util.SocketAddress;

public class FileDownloader {
  
 private static final Logger LOG = Logger.getLogger(FileDownloader.class);
  
  public static boolean downloadFile(SocketAddress dfsAddr, String filePath, String destinationPath) {
    IDFS dfs = DFSFactory.dfsFromSocketAddress(dfsAddr);
    if(dfs == null) {
      System.out.println("Failed to communicate with DFS");
      return false;
    }
    
    FileOutputStream fos = null;
    try {
      fos = new FileOutputStream(destinationPath);
    } catch (IOException e) {
      System.out.println("Failed to open file " + destinationPath);
      LOG.warn("Failed to open file " + destinationPath, e);
      return false;
    }
    
    try {
      FileMetadata metadata = dfs.getMetadata(filePath);
      if(metadata != null) {
        byte [] data = dfs.getFile(filePath, 0);
        if(data != null) {
          fos.write(data);
          fos.close();
          System.out.println("File downloaded successfully!");
        }
        else {
          System.out.println("Failed to find file " + filePath + " in DFS");
        }
      }
      else {
        System.out.println("Couldn't find file in DFS: " + filePath);
      }
    } catch (RemoteException e) {
      System.out.println("Failed to communicate with DFS");
      LOG.warn("Failed to communicate to DFS", e);
      return false;
    } catch (IOException e) {
      System.out.println("Failed to write file " + destinationPath);
      LOG.warn("Failed to write file " + destinationPath, e);
      return false;
    }
    return true;
  }

}
