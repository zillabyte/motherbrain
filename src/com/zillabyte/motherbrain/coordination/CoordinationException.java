package com.zillabyte.motherbrain.coordination;

import com.zillabyte.motherbrain.top.MotherbrainException;

public class CoordinationException extends MotherbrainException {

  private static final long serialVersionUID = 2943295844876339287L;
  
  private String DEFAULT_USER_MESSAGE = "Internal coordination system error.";
  
  public CoordinationException() {
    super();
    super.setUserMessage(DEFAULT_USER_MESSAGE);
  }
  
  public CoordinationException(Exception ex) {
    super(ex);
    super.setUserMessage(DEFAULT_USER_MESSAGE);
  }

  public CoordinationException(String msg) {
    super(msg);
    super.setUserMessage(DEFAULT_USER_MESSAGE);
  }

}
