package com.zillabyte.motherbrain.flow.operations;



public class OperationDeadException extends OperationException {

  private static final long serialVersionUID = -2256710868602170903L;

  public OperationDeadException(Operation o, String s) {
    super(o, s);
  }

  public OperationDeadException(Operation o, Throwable e) {
    super(o, e);
  }
  
}
