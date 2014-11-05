package com.zillabyte.motherbrain.flow.operations.multilang.builder;

import com.zillabyte.motherbrain.container.ContainerException;
import com.zillabyte.motherbrain.container.ContainerWrapper;
import com.zillabyte.motherbrain.flow.config.FlowConfig;
import com.zillabyte.motherbrain.flow.operations.OperationLogger;
import com.zillabyte.motherbrain.universe.Universe;

public class InplaceFlowBuilderFactory implements FlowBuilderFactory {
  
  private static final long serialVersionUID = 4708090849313108014L;

  public FlowFetcher createFlowBuilder(FlowConfig flowConfig, OperationLogger logger) throws ContainerException {
    
    // Create a container..
    ContainerWrapper container = Universe.instance().containerFactory().createContainerFor(flowConfig);
    container.start();
    
    // Create the builder..
    return new InplaceFlowBuilder(flowConfig, container, logger);
  }

}
