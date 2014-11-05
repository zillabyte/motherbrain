package com.zillabyte.motherbrain.flow;

import java.io.Serializable;

import org.apache.commons.lang.NotImplementedException;
import org.apache.log4j.Logger;

import com.zillabyte.motherbrain.coordination.CoordinationException;
import com.zillabyte.motherbrain.flow.error.strategies.FlowErrorStrategy;
import com.zillabyte.motherbrain.flow.operations.Operation;
import com.zillabyte.motherbrain.flow.operations.OperationException;
import com.zillabyte.motherbrain.universe.Universe;
import com.zillabyte.motherbrain.utils.Utils;

public class FlowStateCoordinator implements Serializable {
  
  /**
   * 
   */
  private static final long serialVersionUID = 8973237210512561403L;

  protected App _flow;
  protected FlowErrorStrategy _errorStrategy;
  
  static final Logger _log = Utils.getLogger(FlowStateCoordinator.class);
  
  public FlowStateCoordinator(App flow) {
    _flow = flow;
    _errorStrategy = Universe.instance().errorStrategyFactory().createFlowStrategy();
  }
  
  /***
   * Update the FlowState based on current state of other datums. e.g. this is called
   * anytime anything interesting happens that might make us change FlowState
   * @throws StateMachineException 
   * @throws FlowException 
   * @throws InterruptedException 
   * @throws OperationException 
   * @throws CoordinationException 
   */
  public FlowState maybeGetNewFlowState(final FlowInstanceSetBuilder builder, final FlowState currentState) throws StateMachineException, FlowException, InterruptedException, OperationException, CoordinationException {
        
    /*
     * Do we have any errors in our operations?
     */
    if (currentState != FlowState.ERROR && _errorStrategy.shouldTransitionToFlowError(builder)) {
      return FlowState.ERROR;
    }
    switch (currentState) {
    /*
     * We don't recover from death or ERRORs (the only way is to re-register).
     *
     * Similarly, INITIAL, STARTED and WAITING_FOR_NEXT_CYCLE require explicit
     * user requests before they may be transitioned from.
     */
    case RETIRING:
      // fall through
    case RETIRED:
      // fall through
    case ERRORING:
      // fall through
    case ERROR:
      // fall through
    case KILLING:
      // fall through
    case KILLED:
      // fall through
    case INITIAL:
      // fall through
    case PAUSED:
      // fall through
    case STARTED:
      // fall through
    case WAITING_FOR_NEXT_CYCLE:
      break;
    case RECOVERING:
      // Special recovery states...
      throw new NotImplementedException();
    case PAUSING:
      /*
       * The only way we can go from PAUSING to PAUSED is if all operations report in
       * and say they've finished their pausing sequence(i.e. they are in PAUSED)
       */
      if (builder.assertAtLeastOneInstanceAliveFromEachOperation().withAliveHeartbeats().notInState("ERROR").allInState("PAUSED")) {
        // Yes: everybody is started, so we can transition STARTING to STARTED
        return FlowState.PAUSED;
      }
      break;

    case STARTING:
      /*
       * The only way we can go from STARTING to STARTED is if all operations report in
       * and say they've finished their start up sequence (i.e. they are in STARTED).
       */
      
      // Are all operations online?
      for(Operation op : _flow.getOperations()) {
        if(builder.ofOperation(op).size() != op.getActualParallelism()) return FlowState.STARTING;
      }
      // Is there at least one alive for each operation and is everybody in the STARTED state?
      if (builder.assertAtLeastOneInstanceAliveFromEachOperation().withAliveHeartbeats().notInState("ERROR").allInState("STARTED", "IDLE", "CONSUMING_IDLE")) {
        // Yes: everybody is started, so we can transition STARTING to STARTED
        return FlowState.STARTED;
      }
      break;
    case CYCLE_COMPLETE:

      // Tell the sources that they may enter WAITING_FOR_NEXT_CYCLE, which will allow
      // them to start a new batch when we enter FlowState.RUNNING again
      if (builder.sources().assertAtLeastOneInstanceAliveFromEachOperation().withAliveHeartbeats().notInState("ERROR").allInState("WAITING_FOR_NEXT_CYCLE")) {

        // All operations are have consumed everything possible
        return FlowState.WAITING_FOR_NEXT_CYCLE;

      } else {
        sendCycleAcknowledged();                

      }
      break;
    case IDLE:
      // Only RPCs should be in idle, if the source goes back to EMITTING, then the flow should go back to RUNNING.
      if (builder.sources().assertAtLeastOneInstanceAliveFromEachOperation().anyInState("EMITTING") ||
          builder.nonSources().assertAtLeastOneInstanceAliveFromEachOperation().anyInState("ACTIVE", "CONSUMING", "EMITTING")) {
        return FlowState.RUNNING;
      }
      break;
    case RUNNING:
      
      // Detect when all of the tuples are out of the system
      if (builder.sources().assertAtLeastOneInstanceAliveFromEachOperation().withAliveHeartbeats().notInState("ERROR").allInState("EMIT_COMPLETE", "EMIT_COMPLETE_ACKED", "IDLE")) {
        
        // If all non-sources are done processing, we can get out of RUNNING state
        if (builder.nonSources().assertAtLeastOneInstanceAliveFromEachOperation().withAliveHeartbeats().notInState("ERROR").allInState("IDLE", "EMITTING_DONE") ) {
          if (builder.sources().assertAtLeastOneInstanceAliveFromEachOperation().withAliveHeartbeats().notInState("ERROR").allInState("IDLE")) {
            // Sources are in IDLE => RPC, we idle the flow in this case
            return FlowState.IDLE;
          } else {
            // builder.debugStates();
            // Otherwise we're a regular app, and we complete the cycle
            return FlowState.CYCLE_COMPLETE;
          }
        }
      }

      break;
    default:
      _log.error("Current flow state is invalid: " + currentState);
      break;
    }
    return currentState;
  }

  
  
  /***
   * For testing 
   * @param string
   * @throws CoordinationException 
   */
  protected void sendCycleAcknowledged() throws CoordinationException {
    Universe.instance().state().sendMessage(_flow.flowStateKey() + "/operation_commands", "cycle_acknowledged");
  }
  
}
