package com.zillabyte.motherbrain.flow.buffer;

import com.zillabyte.motherbrain.flow.MapTuple;

/**
 * 
 * @author sashi
 *
 */
public interface BufferProducer {
  public void pushTuple(MapTuple t);
}
