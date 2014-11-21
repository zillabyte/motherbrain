package com.zillabyte.motherbrain.reactor.lightweight;

import com.zillabyte.motherbrain.top.MotherbrainException;

public class ProcessTimeoutException extends MotherbrainException {


  private static final long serialVersionUID = 2842787168157005878L;

  

  public ProcessTimeoutException(ProcessTimeoutException e) {
    super(e);
  }



  public ProcessTimeoutException(String message) {
    super(message);
  }
}
