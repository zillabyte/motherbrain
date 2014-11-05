package grandmotherbrain.flow.operations.multilang.builder;

import grandmotherbrain.container.ContainerException;
import grandmotherbrain.container.ContainerWrapper;
import grandmotherbrain.flow.config.FlowConfig;
import grandmotherbrain.flow.operations.OperationLogger;
import grandmotherbrain.universe.Universe;

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
