package com.zillabyte.motherbrain.api;

import com.zillabyte.motherbrain.top.MotherbrainException;

public class APIException extends MotherbrainException {
 
  private static final long serialVersionUID = -6070070564404552728L;
  private final String DEFAULT_USER_MESSAGE = "Internal API error.";
  
  public APIException() {
    super();
    super.setUserMessage(DEFAULT_USER_MESSAGE);
  }

  public APIException(String string) {
    super(string);
    super.setUserMessage(DEFAULT_USER_MESSAGE);
  }
  
  public APIException(Throwable e) {
    super(e);
    super.setUserMessage(DEFAULT_USER_MESSAGE);
  }

  public APIException(Exception e) {
    super(e);
    super.setUserMessage(DEFAULT_USER_MESSAGE);
  }
  
  public APIException setUserMessage(String e) {
    return (APIException) super.setUserMessage(e);
  }

}
