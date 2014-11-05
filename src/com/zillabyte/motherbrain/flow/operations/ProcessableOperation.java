package com.zillabyte.motherbrain.flow.operations;

import com.zillabyte.motherbrain.flow.MapTuple;
import com.zillabyte.motherbrain.flow.collectors.OutputCollector;

public interface ProcessableOperation {
  public void handleProcess(MapTuple t, OutputCollector c) throws Exception;
}
