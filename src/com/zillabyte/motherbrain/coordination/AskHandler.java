package com.zillabyte.motherbrain.coordination;


public interface AskHandler {

  public Object handleAsk(String key, Object payload);  
  
}
