package com.zillabyte.motherbrain.coordination.redis;

import com.zillabyte.motherbrain.coordination.CoordinationException;

public class RedisException extends CoordinationException {

  /**
   * 
   */
  private static final long serialVersionUID = -5206696879046798295L;

  public RedisException() {
    super();
  }
  
  public RedisException(Exception ex) {
    super(ex);
  }

  public RedisException(String msg) {
    super(msg);
  }

}
