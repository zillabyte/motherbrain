package com.zillabyte.motherbrain.container;

import com.zillabyte.motherbrain.top.MotherbrainException;

public class CachedFlowException extends MotherbrainException {

  public CachedFlowException(String message) {
    super(message);
  }
  
  public CachedFlowException(Exception e) {
    super(e.getMessage(), e);
  }
}
