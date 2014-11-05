package com.zillabyte.motherbrain.flow.aggregation;

import com.zillabyte.motherbrain.flow.MapTuple;
import com.zillabyte.motherbrain.flow.collectors.OutputCollector;
import com.zillabyte.motherbrain.top.MotherbrainException;

public interface Aggregator {

  public abstract void start(MapTuple newGroupFieldValues) throws MotherbrainException, InterruptedException;
  
  public abstract void aggregate(MapTuple t, OutputCollector c) throws MotherbrainException, InterruptedException;
  
  public abstract void complete(OutputCollector c) throws MotherbrainException, InterruptedException;

  
}
