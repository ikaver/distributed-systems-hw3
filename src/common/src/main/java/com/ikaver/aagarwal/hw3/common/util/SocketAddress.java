package com.ikaver.aagarwal.hw3.common.util;

import java.io.Serializable;

public class SocketAddress implements Serializable {
  
  private static final long serialVersionUID = 5627039049875852720L;
  
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
  
  @Override
  public String toString() {
    return String.format("[%s:%d]",hostname,port);
  }
  
  @Override
  public boolean equals(Object o) {
    if(o == null) return false;
    if(!(o instanceof SocketAddress)) return false;
    SocketAddress other = (SocketAddress)o;
    return getHostname().equals(other.getHostname()) && getPort() == other.getPort();
  }
  
  @Override
  public int hashCode() {
    return 37 * getHostname().hashCode() +  new Integer(getPort()).hashCode();
  }
}
