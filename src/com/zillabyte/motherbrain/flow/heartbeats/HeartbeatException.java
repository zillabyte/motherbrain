package com.zillabyte.motherbrain.flow.heartbeats;

import com.zillabyte.motherbrain.top.MotherbrainException;

public class HeartbeatException extends MotherbrainException {

  private static final long serialVersionUID = 2712555514330403215L;

  public HeartbeatException(Exception e) {
    super(e);
  }

  public HeartbeatException(String string) {
    super(string);
  }
}
