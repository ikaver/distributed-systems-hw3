package com.ikaver.aagarwal.hw3.mrclient.jobmonitor.commandhandler;

import com.ikaver.aagarwal.hw3.common.commandhandler.ICommandHandler;

public class EndClientSystemCommandHandler  implements ICommandHandler {

  public EndClientSystemCommandHandler() { }

  public boolean handleCommand(String[] args) {
    if(args.length < 1) return false;
    
    System.out.println("Goodbye!");
    System.exit(0);
    return true;
  }

  public String helpString() {
    return "";
  }
}
