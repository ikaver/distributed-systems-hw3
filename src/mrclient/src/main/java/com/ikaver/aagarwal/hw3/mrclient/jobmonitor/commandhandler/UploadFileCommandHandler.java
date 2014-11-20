package com.ikaver.aagarwal.hw3.mrclient.jobmonitor.commandhandler;

import com.ikaver.aagarwal.hw3.common.commandhandler.ICommandHandler;
import com.ikaver.aagarwal.hw3.common.util.SocketAddress;
import com.ikaver.aagarwal.hw3.mrclient.dfs.FileUploader;

public class UploadFileCommandHandler implements ICommandHandler {
    
  private SocketAddress masterSocketAddr;
  
  public UploadFileCommandHandler(SocketAddress masterAddr) {
    this.masterSocketAddr = masterAddr;
  }

  public boolean handleCommand(String[] args) {
    if(args.length < 4) return false;
    String inputFilePath = args[1];
    String destinationPath = args[2];
    int recordSize = -1;
    try{
      recordSize = Integer.parseInt(args[3]);
    }
    catch(NumberFormatException e) {
      System.out.println("Record size must be an integer");
      return false;
    }
    FileUploader.uploadFile(masterSocketAddr, inputFilePath, destinationPath, recordSize);
    return true; 
  }

  public String helpString() {
    return "<INPUT-FILE-PATH> <DESTINATION-PATH>";
  }
}
