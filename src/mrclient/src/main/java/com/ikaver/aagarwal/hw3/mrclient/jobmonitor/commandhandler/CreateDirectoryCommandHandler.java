package com.ikaver.aagarwal.hw3.mrclient.jobmonitor.commandhandler;

import java.rmi.RemoteException;

import org.apache.log4j.Logger;

import com.ikaver.aagarwal.hw3.common.commandhandler.ICommandHandler;
import com.ikaver.aagarwal.hw3.common.dfs.IDFS;
import com.ikaver.aagarwal.hw3.common.util.SocketAddress;
import com.ikaver.aagarwal.hw3.mrdfs.master.DFSFactory;


public class CreateDirectoryCommandHandler implements ICommandHandler {
  private static final Logger LOG = Logger.getLogger(UploadFileCommandHandler.class);
  
  private SocketAddress masterSocketAddr;
  
  public CreateDirectoryCommandHandler(SocketAddress masterAddr) {
    this.masterSocketAddr = masterAddr;
  }

  public boolean handleCommand(String[] args) {
    if(args.length < 2) return false;
    String directoryPath = args[1];
        
    IDFS dfs = DFSFactory.dfsFromSocketAddress(masterSocketAddr);
    if(dfs == null) {
      System.out.println("Failed to locate DFS master. Try again later.");
    }
    else {
      try {
        boolean success = dfs.createDirectory(directoryPath);
        if(success) {
          System.out.println("Directory created successfully");
        }
        else {
          System.out.println("DFS couldn't create directory. Make sure the directory doesn't already exist.");
        }
      } catch (RemoteException e) {
        System.out.println("Failed to communicate with DFS.");
        LOG.warn("Failed to communicate with DFS", e);
      }
    }
    return true;
  }

  public String helpString() {
    return "<DIRECTORYPATH>";
  }
}
