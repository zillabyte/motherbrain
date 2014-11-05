package grandmotherbrain.flow.error.strategies;

import grandmotherbrain.flow.operations.Operation;
import grandmotherbrain.flow.operations.OperationException;

public class ErrorThresholdExceededException extends OperationException {


  private static final long serialVersionUID = 3215975692168353056L;

  
  public ErrorThresholdExceededException(Operation op, Throwable e) {
    super(op, e);
  }

}
