package com.zillabyte.motherbrain.flow.aggregation;

import com.zillabyte.motherbrain.flow.MapTuple;
import com.zillabyte.motherbrain.flow.collectors.OutputCollector;
import com.zillabyte.motherbrain.flow.operations.LoopException;

public interface Aggregator {

  public abstract void start(MapTuple newGroupFieldValues) throws LoopException;
  
  public abstract void aggregate(MapTuple t, OutputCollector c) throws LoopException;
  
  public abstract void complete(OutputCollector c) throws LoopException;

  
}
