package com.zillabyte.motherbrain.flow.error.strategies;

import com.zillabyte.motherbrain.flow.operations.Operation;
import com.zillabyte.motherbrain.flow.operations.OperationException;

public class ErrorThresholdExceededException extends OperationException {


  private static final long serialVersionUID = 3215975692168353056L;

  
  public ErrorThresholdExceededException(Operation op, Throwable e) {
    super(op, e);
  }

}
