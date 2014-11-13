package com.zillabyte.motherbrain.flow.buffer;

import net.sf.json.JSONObject;

import com.zillabyte.motherbrain.flow.MapTuple;

/**
 * 
 * @author sashi
 *
 */
public interface BufferConsumer {

  public MapTuple getNextTuple();
  public boolean isEmitComplete();
  
  public JSONObject createSnapshot();
  public void applySnapshot(JSONObject snapshot);
  
} 
