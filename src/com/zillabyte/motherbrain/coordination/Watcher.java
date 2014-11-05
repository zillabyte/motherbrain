package com.zillabyte.motherbrain.coordination;


public interface Watcher {

  public void unsubscribe() throws CoordinationException;
  
}
