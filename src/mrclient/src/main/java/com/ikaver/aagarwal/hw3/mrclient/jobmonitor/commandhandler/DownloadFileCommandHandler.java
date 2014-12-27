package com.ikaver.aagarwal.hw3.mrclient.jobmonitor.commandhandler;

import com.ikaver.aagarwal.hw3.common.commandhandler.ICommandHandler;
import com.ikaver.aagarwal.hw3.common.util.SocketAddress;
import com.ikaver.aagarwal.hw3.mrclient.dfs.FileDownloader;
import com.ikaver.aagarwal.hw3.mrclient.dfs.FileDownloader.DownloadFileResult;

public class DownloadFileCommandHandler implements ICommandHandler {

  private SocketAddress masterSocketAddr;

  public DownloadFileCommandHandler(SocketAddress masterAddr) {
    this.masterSocketAddr = masterAddr;
  }

  public boolean handleCommand(String[] args) {
    if (args.length < 3)
      return false;
    String remoteFilePath = args[1];
    String destinationPath = args[2];
    System.out.println("Starting download process, please wait...");
    DownloadFileResult result = FileDownloader.downloadFile(masterSocketAddr, remoteFilePath, destinationPath);
    if(result == DownloadFileResult.SUCCESS) {
      System.out.println("File downloaded successfully!");
    }
    else if(result == DownloadFileResult.FAILED_TO_FIND_FILE_IN_DFS) {
      System.out.println("Couldn't find file in DFS: " + remoteFilePath);
    }
    else if(result == DownloadFileResult.FAILED_TO_OPEN_FILE) {
      System.out.println("Failed to open file " + remoteFilePath);
    }
    else if(result == DownloadFileResult.FAILED_TO_WRITE_FILE) {
      System.out.println("Failed to write file " + remoteFilePath);
    }
    else if(result == DownloadFileResult.DFS_COMM_FAILURE) {
      System.out.println("Failed to communicate with DFS");
    }
    return true;
  }

  public String helpString() {
    return "<REMOTE-FILE-PATH> <LOCAL-DESTINATION-PATH>";
  }

}
