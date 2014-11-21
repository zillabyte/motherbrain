package com.zillabyte.motherbrain.flow.error.strategies;

import com.zillabyte.motherbrain.flow.operations.LoopException;

public class OperationErrorStrategyWrapper implements OperationErrorStrategy {
  OperationErrorStrategy _delegate;
  
  public OperationErrorStrategyWrapper(OperationErrorStrategy delegate) {
    _delegate = delegate;
  }
  
  @Override
  public void handleLoopError(LoopException error) throws FakeLocalException {
    try {
      _delegate.handleLoopError(error);
    } catch (FakeLocalException e) {
      throw e;
    } catch (Exception e) {
      _delegate.handleFatalError(error);
    }
  }

  @Override
  public void handleFatalError(Throwable error) throws FakeLocalException {
    _delegate.handleFatalError(error);
  }

  @Override
  public long getErrorCount() {
    return _delegate.getErrorCount();
  }

  @Override
  public void handleHeartbeatDeath() throws FakeLocalException {
    _delegate.handleHeartbeatDeath();
  }

}
