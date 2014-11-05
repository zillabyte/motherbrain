package grandmotherbrain.flow.operations.multilang.builder;

import grandmotherbrain.container.ContainerException;
import grandmotherbrain.flow.config.FlowConfig;
import grandmotherbrain.flow.operations.OperationLogger;

import java.io.Serializable;

public interface FlowBuilderFactory extends Serializable   {


  public FlowFetcher createFlowBuilder(FlowConfig flowConfig, OperationLogger logger) throws ContainerException;

  
  
}
