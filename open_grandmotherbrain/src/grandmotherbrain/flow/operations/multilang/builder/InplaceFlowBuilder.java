package grandmotherbrain.flow.operations.multilang.builder;

import grandmotherbrain.container.ContainerWrapper;
import grandmotherbrain.container.local.InplaceContainer;
import grandmotherbrain.flow.Flow;
import grandmotherbrain.flow.FlowCompilationException;
import grandmotherbrain.flow.config.FlowConfig;
import grandmotherbrain.flow.operations.OperationLogger;
import grandmotherbrain.utils.Utils;
import net.sf.json.JSONObject;

import org.apache.log4j.Logger;

public class InplaceFlowBuilder  implements FlowFetcher {

  private static final long serialVersionUID = 5321655302235740392L;
  private static final Logger _log = Utils.getLogger(InplaceFlowBuilder.class);
  
  private MultilangFlowCompiler _flowCompiler;
  private OperationLogger _logger;
  private ContainerWrapper _container;
  private FlowConfig _flowConfig;
  
  
  /***
   * 
   * @param authToken
   */
  public InplaceFlowBuilder(FlowConfig flowConfig, ContainerWrapper destContainer, OperationLogger logger) {
    
    // Init 
    _flowCompiler = new MultilangFlowCompiler(this, flowConfig, destContainer, logger);
    _flowConfig = flowConfig;
    _logger = logger;
    _container = destContainer;
    
    // Sanity
    if (_container.getDelegate() instanceof InplaceContainer == false) {
      throw new IllegalStateException("InPlaceFlowBuilder will only work with InPlaceContainers!");
    }
  }
  
  
  
  
  /**
   * 
   * @param id
   * @param flowLogger
   * @param destDir
   * @throws InterruptedException
   * @throws LXCException
   * @throws FlowCompilationException 
   */
  @Override
  public Flow buildFlow(String flowName, JSONObject overrideConfig) throws FlowCompilationException {
    try {
      
      // Actually build the flow
      return _flowCompiler.compileFlow(flowName, overrideConfig);
        
    } catch(Exception e) {
      throw new FlowCompilationException(e);
    }
  }

  
  
  public Flow buildFlow(String flowName) throws FlowCompilationException {
    return buildFlow(flowName, new JSONObject());
  }

  
}
