package com.ikaver.aagarwal.hw3.common.commandhandler;

public interface ICommandHandler {
  public boolean handleCommand(String [] args); 
  public String helpString();
}
