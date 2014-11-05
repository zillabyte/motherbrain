package com.zillabyte.motherbrain.flow.aggregation;

import java.io.Serializable;

import com.zillabyte.motherbrain.flow.operations.AggregationOperation;

public interface AggregationStoreFactory extends Serializable {

  public AggregationStoreWrapper getStore(AggregationOperation op, String prefix);
  
}
