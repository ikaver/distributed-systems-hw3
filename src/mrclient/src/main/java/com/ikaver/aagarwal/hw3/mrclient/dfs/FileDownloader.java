package com.ikaver.aagarwal.hw3.mrclient.dfs;

import java.io.FileOutputStream;
import java.io.IOException;
import java.rmi.RemoteException;

import org.apache.log4j.Logger;

import com.ikaver.aagarwal.hw3.common.dfs.DFSFactory;
import com.ikaver.aagarwal.hw3.common.dfs.FileMetadata;
import com.ikaver.aagarwal.hw3.common.dfs.IDFS;
import com.ikaver.aagarwal.hw3.common.util.SocketAddress;

/**
 * File downloader for the client. Downloads from the DFS and stores them 
 * on the client file system.
 */
public class FileDownloader {
  
  public enum DownloadFileResult {
    SUCCESS,
    DFS_COMM_FAILURE,
    FAILED_TO_OPEN_FILE,
    FAILED_TO_WRITE_FILE,
    FAILED_TO_FIND_FILE_IN_DFS
  }

  private static final Logger LOG = Logger.getLogger(FileDownloader.class);

  public static DownloadFileResult downloadFile(SocketAddress dfsAddr, String filePath, String destinationPath) {
    IDFS dfs = DFSFactory.dfsFromSocketAddress(dfsAddr);
    if(dfs == null) {
      return DownloadFileResult.DFS_COMM_FAILURE;
    }

    FileOutputStream fos = null;
    DownloadFileResult result = DownloadFileResult.SUCCESS;
    try {
      fos = new FileOutputStream(destinationPath);
    } catch (IOException e) {
      LOG.warn("Failed to open file " + destinationPath, e);
      return DownloadFileResult.FAILED_TO_OPEN_FILE;
    }

    try {
      FileMetadata metadata = dfs.getMetadata(filePath);
      if(metadata != null) {
        byte [] data = dfs.getFile(filePath, 0);
        if(data != null) {
          fos.write(data);
          result = DownloadFileResult.SUCCESS;
        }
        else {
          result = DownloadFileResult.FAILED_TO_FIND_FILE_IN_DFS;
        }
      }
      else {
        result = DownloadFileResult.FAILED_TO_FIND_FILE_IN_DFS;
      }
      if(fos != null) fos.close();
    } catch (RemoteException e) {
      LOG.warn("Failed to communicate to DFS", e);
      result = DownloadFileResult.DFS_COMM_FAILURE;
    } catch (IOException e) {
      LOG.warn("Failed to write file " + destinationPath, e);
      result =  DownloadFileResult.FAILED_TO_WRITE_FILE;
    }
    return result;
  }

}
