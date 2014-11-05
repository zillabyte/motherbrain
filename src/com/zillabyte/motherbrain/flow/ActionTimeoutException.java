package com.zillabyte.motherbrain.flow;

import com.zillabyte.motherbrain.flow.operations.Operation;
import com.zillabyte.motherbrain.flow.operations.OperationException;

public class ActionTimeoutException extends OperationException {

  /**
   * 
   */
  private static final long serialVersionUID = -5311969934287427423L;

  public ActionTimeoutException(Operation o, String string) {
    super(o, string);
  }

}
