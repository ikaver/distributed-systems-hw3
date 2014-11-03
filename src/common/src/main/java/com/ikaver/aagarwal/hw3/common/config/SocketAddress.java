package com.ikaver.aagarwal.hw3.common.config;

public class SocketAddress {
  
  private final String hostname;
  private final int port;
  
  public SocketAddress(String hostname, int port) {
    this.hostname = hostname;
    this.port = port;
  }
  
  public String getHostname() {
    return hostname;
  }

  public int getPort() {
    return port;
  }

}
