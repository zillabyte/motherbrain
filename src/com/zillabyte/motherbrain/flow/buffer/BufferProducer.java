package com.zillabyte.motherbrain.flow.buffer;

import com.zillabyte.motherbrain.flow.MapTuple;
import com.zillabyte.motherbrain.flow.operations.LoopException;

/**
 * 
 * @author sashi
 *
 */
public interface BufferProducer {
  public void pushTuple(MapTuple t) throws LoopException;
}
