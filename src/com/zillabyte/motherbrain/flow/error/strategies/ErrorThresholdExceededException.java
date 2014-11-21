package com.zillabyte.motherbrain.flow.error.strategies;

import com.zillabyte.motherbrain.flow.operations.LoopException;
import com.zillabyte.motherbrain.flow.operations.Operation;

public class ErrorThresholdExceededException extends LoopException {


  private static final long serialVersionUID = 3215975692168353056L;

  
  public ErrorThresholdExceededException(Operation op, Throwable e) {
    super(op, e);
  }

}
