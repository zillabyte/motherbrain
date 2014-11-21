package com.zillabyte.motherbrain.flow.error.strategies;

import com.zillabyte.motherbrain.flow.operations.LoopException;

public interface OperationErrorStrategy {

  
  void handleLoopError(LoopException error) throws FakeLocalException;

  void handleFatalError(Throwable error) throws FakeLocalException;

  long getErrorCount();

  void handleHeartbeatDeath() throws FakeLocalException;
  
}
