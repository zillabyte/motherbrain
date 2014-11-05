package grandmotherbrain.flow.operations.multilang.builder;

import grandmotherbrain.container.ContainerException;
import grandmotherbrain.container.ContainerWrapper;
import grandmotherbrain.flow.config.FlowConfig;
import grandmotherbrain.flow.operations.OperationLogger;
import grandmotherbrain.universe.Universe;

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