package com.zillabyte.motherbrain.flow.operations.multilang.builder;

import com.zillabyte.motherbrain.container.ContainerException;
import com.zillabyte.motherbrain.container.ContainerWrapper;
import com.zillabyte.motherbrain.flow.config.FlowConfig;
import com.zillabyte.motherbrain.flow.operations.OperationLogger;
import com.zillabyte.motherbrain.universe.Universe;

/***
 * 
 * @author jake
 *
 */
public class APIFlowBuilderFactory implements FlowBuilderFactory {
  /**
   * 
   */
  private static final long serialVersionUID = 8112933766394898037L;

  public FlowFetcher createFlowBuilder(FlowConfig flowConfig, OperationLogger logger) throws ContainerException {
    
    // Create a container..
    ContainerWrapper container = Universe.instance().containerFactory().createContainerFor(flowConfig);
    container.start();
    
    // Create the builder..
    return new APIFlowBuilder(flowConfig, container, logger);
  }
}