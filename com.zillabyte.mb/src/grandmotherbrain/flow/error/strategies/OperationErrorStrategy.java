package grandmotherbrain.flow.error.strategies;

import grandmotherbrain.flow.operations.OperationException;

public interface OperationErrorStrategy {

  
  void handleLoopError(Throwable error) throws OperationException, FakeLocalException;

  void handleFatalError(Throwable error) throws OperationException, FakeLocalException;

  long getErrorCount();

  Throwable maybeGetFatalError();

  void handleHeartbeatDeath() throws OperationException, FakeLocalException;
  
}
