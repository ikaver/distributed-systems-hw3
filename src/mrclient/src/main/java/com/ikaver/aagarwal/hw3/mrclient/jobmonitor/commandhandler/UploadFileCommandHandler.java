package com.ikaver.aagarwal.hw3.mrclient.jobmonitor.commandhandler;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.rmi.RemoteException;

import org.apache.log4j.Logger;

import com.ikaver.aagarwal.hw3.common.commandhandler.ICommandHandler;
import com.ikaver.aagarwal.hw3.common.dfs.IDFS;
import com.ikaver.aagarwal.hw3.common.util.SocketAddress;
import com.ikaver.aagarwal.hw3.mrdfs.master.DFSFactory;

public class UploadFileCommandHandler implements ICommandHandler {
  
  private static final Logger LOG = Logger.getLogger(UploadFileCommandHandler.class);
  
  private SocketAddress masterSocketAddr;
  
  public UploadFileCommandHandler(SocketAddress masterAddr) {
    this.masterSocketAddr = masterAddr;
  }

  public boolean handleCommand(String[] args) {
    if(args.length < 3) return false;
    String inputFilePath = args[1];
    String destinationPath = args[2];
    
    File inputFile = new File(inputFilePath);
    byte[] data = new byte[(int)inputFile.length()];
    try {
      FileInputStream fis = new FileInputStream(inputFile);
      fis.read(data);
      fis.close();
    } catch (IOException e) {
      System.out.println("Failed to read file: " + inputFilePath);
      return true;
    }
        
    IDFS dfs = DFSFactory.dfsFromSocketAddress(masterSocketAddr);
    if(dfs == null) {
      System.out.println("Failed to locate DFS master. Try again later.");
    }
    else {
      try {
        boolean success = dfs.saveFile(destinationPath, data);
        if(success) {
          System.out.println("File uploaded successfully");
        }
        else {
          System.out.println("DFS rejected file. Make sure the file path is valid.");
        }
      } catch (RemoteException e) {
        System.out.println("Failed to communicate with DFS.");
        LOG.warn("Failed to communicate with DFS", e);
      }
    }
    return true;
  }

  public String helpString() {
    return "<INPUT-FILE-PATH> <DESTINATION-PATH>";
  }
}
