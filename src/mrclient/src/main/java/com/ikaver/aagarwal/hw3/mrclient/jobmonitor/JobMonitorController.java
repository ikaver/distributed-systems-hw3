package com.ikaver.aagarwal.hw3.mrclient.jobmonitor;

import java.util.Map;
import java.util.Scanner;

import com.google.inject.Inject;
import com.ikaver.aagarwal.hw3.common.commandhandler.ICommandHandler;
import com.ikaver.aagarwal.hw3.common.commandhandler.ICommandHandlerFactory;

public class JobMonitorController {

  private Map<String, ICommandHandler> commandHandlers;

  @Inject
  public JobMonitorController(ICommandHandlerFactory factory) {
    this.commandHandlers = factory.getCommandHandlers();
  }

  private void printHelp() {
    System.out.println("Job Monitor.");
    for(String key : commandHandlers.keySet()) {
      System.out.printf("\t%s %s\n", key, commandHandlers.get(key).helpString());
    }
  }

  private void processLine(String line) {
    if(line == null) return;
    String [] tokens = line.split("\\s+");
    if(tokens == null || tokens.length == 0) return;
    String command = tokens[0];
    boolean isCorrectCommand = false;
    if(commandHandlers.containsKey(command) 
        && commandHandlers.get(command).handleCommand(tokens)) {
      isCorrectCommand = true;
    }
    if(!isCorrectCommand) {
      printHelp();
    }
  }

  public void start() {
    Scanner s = new Scanner(System.in);
    while(s.hasNext()) {
      System.out.print("> ");
      processLine(s.nextLine());
    }
    s.close();
  }

}
