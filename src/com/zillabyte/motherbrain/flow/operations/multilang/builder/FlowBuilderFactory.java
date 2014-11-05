package com.zillabyte.motherbrain.flow.operations.multilang.builder;

import java.io.Serializable;

import com.zillabyte.motherbrain.container.ContainerException;
import com.zillabyte.motherbrain.flow.config.FlowConfig;
import com.zillabyte.motherbrain.flow.operations.OperationLogger;

public interface FlowBuilderFactory extends Serializable   {


  public FlowFetcher createFlowBuilder(FlowConfig flowConfig, OperationLogger logger) throws ContainerException;

  
  
}
