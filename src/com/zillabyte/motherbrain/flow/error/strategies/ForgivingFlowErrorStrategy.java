package com.zillabyte.motherbrain.flow.error.strategies;

import java.io.Serializable;
import java.util.Set;

import org.apache.log4j.Logger;

import com.zillabyte.motherbrain.flow.FlowInstanceSetBuilder;
import com.zillabyte.motherbrain.utils.Utils;


public final class ForgivingFlowErrorStrategy implements FlowErrorStrategy, Serializable { 


  private static final long serialVersionUID = -4153717009098010433L;
  private static Logger _log = Utils.getLogger(ForgivingFlowErrorStrategy.class);
  
  
  /****
   * @param setBuilder
   * @return T if the flow should upgrade to ERROR
   */
  @Override
  public boolean shouldTransitionToFlowError(final FlowInstanceSetBuilder setBuilder) {
    
    
    // We transition to error if ALL instances of a given operation are in ERROR
    Set<String> allOperations = setBuilder.operationIds();
    
    // Iterate each operation with an error.. 
    for(String opId : allOperations) {
      FlowInstanceSetBuilder opSetBuilder = setBuilder.ofOperation(opId).withAliveHeartbeats();
      // If there are more than 2 instances in ERROR, flow should ERROR
      if (opSetBuilder.inState("ERROR").size() > 2) {
        _log.info("ERROR instance threshold exceeded.");
        return true;
      }
      // If all instances are in ERROR or SUSPECT, flow should ERROR
      if (opSetBuilder.allInState("ERROR", "SUSPECT")) {
        _log.info("All instances of operation '" + opId + "' are in state SUSPECT or ERROR");
        return true; 
      }
      // If any instance is in ERROR during a startup stage, flow should ERROR
      if (opSetBuilder.anyInState("ERROR") && opSetBuilder.anyInState("INITIAL", "STARTING", "STARTED")) {
        _log.info("ERROR during prepare stage.");
        return true;
      }
      
    }
    
    // Otherwise, don't transition to ERROR
    return false;
    
  }
}