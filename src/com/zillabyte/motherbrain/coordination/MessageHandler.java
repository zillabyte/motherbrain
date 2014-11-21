package com.zillabyte.motherbrain.coordination;

import com.zillabyte.motherbrain.flow.operations.LoopException;

public interface MessageHandler {

  /***
   * handles new message
   * @param key
   * @throws LoopException 
   * @throws InterruptedException 
   * @throws CoordinationException 
   */
  public void handleNewMessage(String key, Object payload);
 
  
}
