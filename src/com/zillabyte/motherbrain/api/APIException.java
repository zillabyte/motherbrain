package com.zillabyte.motherbrain.api;

import com.zillabyte.motherbrain.top.MotherbrainException;

public class APIException extends MotherbrainException {
 
  private static final long serialVersionUID = -6070070564404552728L;
  
  public APIException(String string) {
    super(string);
  }
  
  public APIException(Throwable e) {
    super(e.getMessage(), e);
  }

  public APIException(Exception e) {
    super(e.getMessage(), e);
  }

}
