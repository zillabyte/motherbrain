package com.zillabyte.motherbrain.flow.error.strategies;

import com.zillabyte.motherbrain.flow.operations.OperationException;

public interface OperationErrorStrategy {

  
  void handleLoopError(Throwable error) throws OperationException, FakeLocalException;

  void handleFatalError(Throwable error) throws OperationException, FakeLocalException;

  long getErrorCount();

  Throwable maybeGetFatalError();

  void handleHeartbeatDeath() throws OperationException, FakeLocalException;
  
}
