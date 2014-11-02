package com.ikaver.aagarwal.hw3.common.commandhandler;

import java.util.Map;

public interface ICommandHandlerFactory {
  
  public Map<String, ICommandHandler> getCommandHandlers();

}
