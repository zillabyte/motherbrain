package grandmotherbrain.flow.error.strategies;

import grandmotherbrain.flow.FlowInstanceSetBuilder;
import grandmotherbrain.utils.Utils;

import java.io.Serializable;
import java.util.Set;

import org.apache.log4j.Logger;

@Deprecated() // This strategy is good for small-scale debugging, but will cause issues at scale
public final class StrictFlowErrorStrategy implements FlowErrorStrategy, Serializable { 

  /**
   * 
   */
  private static final long serialVersionUID = 1156986045324116461L;
  private static Logger _log = Utils.getLogger(StrictFlowErrorStrategy.class);
  
  
  
  /****
   * @param setBuilder
   * @return T if the flow should upgrade to ERROR
   */
  @Override
  public boolean shouldTransitionToFlowError(final FlowInstanceSetBuilder setBuilder) {
    
    // We transition to error if any operations have a ERROR state or all instances of an operation are dead
    if(setBuilder.anyInState("ERROR")){
      _log.info("There's an operation instance in error, transitioning flow to ERROR");
      return true;
    }
    if(!setBuilder.allOperationsAlive()) {
      _log.info("Not all operations are alive, transitioning flow to ERROR");
      return true;
    }
    
    // We transition to error if ALL instances of a given operation are in SUSPECT
    Set<String> allOperations = setBuilder.operationIds();
    
    // Iterate each operation with an error.. 
    for(String opId : allOperations) { 
      
      if (setBuilder.ofOperation(opId).withAliveHeartbeats().allInState("SUSPECT")) {
        _log.info("All instances of operation '" + opId + "' are in state SUSPECT");
        return true; 
      }
      
    }
    
    // Otherwise, don't transition to ERROR
    return false;
    
  }
}