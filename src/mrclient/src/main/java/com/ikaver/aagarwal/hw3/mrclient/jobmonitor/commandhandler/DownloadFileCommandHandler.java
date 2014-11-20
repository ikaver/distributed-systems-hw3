package com.ikaver.aagarwal.hw3.mrclient.jobmonitor.commandhandler;

import com.ikaver.aagarwal.hw3.common.commandhandler.ICommandHandler;
import com.ikaver.aagarwal.hw3.common.util.SocketAddress;
import com.ikaver.aagarwal.hw3.mrclient.dfs.FileDownloader;

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
    FileDownloader.downloadFile(masterSocketAddr, remoteFilePath, destinationPath);
    return true;
  }

  public String helpString() {
    return "<REMOTE-FILE-PATH> <LOCAL-DESTINATION-PATH>";
  }

}
