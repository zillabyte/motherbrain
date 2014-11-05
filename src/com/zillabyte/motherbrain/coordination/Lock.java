package com.zillabyte.motherbrain.coordination;

public interface Lock {

  public void release() throws CoordinationException;
  
}
