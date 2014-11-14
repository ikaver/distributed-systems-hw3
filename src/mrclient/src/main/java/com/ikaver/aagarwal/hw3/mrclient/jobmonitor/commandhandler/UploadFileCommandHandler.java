package com.ikaver.aagarwal.hw3.mrclient.jobmonitor.commandhandler;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.rmi.RemoteException;

import org.apache.log4j.Logger;

import com.ikaver.aagarwal.hw3.common.commandhandler.ICommandHandler;
import com.ikaver.aagarwal.hw3.common.dfs.IDFS;
import com.ikaver.aagarwal.hw3.common.util.SocketAddress;
import com.ikaver.aagarwal.hw3.mrclient.fileuploader.FileUploader;
import com.ikaver.aagarwal.hw3.mrdfs.master.DFSFactory;

public class UploadFileCommandHandler implements ICommandHandler {
  
  private static final Logger LOG = Logger.getLogger(UploadFileCommandHandler.class);
  
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
