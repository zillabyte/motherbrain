package com.zillabyte.motherbrain.flow.collectors.coordinated.support;

import java.io.Serializable;

import com.zillabyte.motherbrain.flow.MapTuple;

public interface TupleIdGenerator extends Serializable {

  public Object getTupleIdFor(MapTuple t);
  
}
