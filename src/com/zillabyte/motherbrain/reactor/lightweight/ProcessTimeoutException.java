package com.zillabyte.motherbrain.reactor.lightweight;

public class ProcessTimeoutException extends Exception {


  private static final long serialVersionUID = 2842787168157005878L;

  

  public ProcessTimeoutException(ProcessTimeoutException e) {
    super(e);
  }



  public ProcessTimeoutException() {
    // TODO Auto-generated constructor stub
  }
}
