package com.zillabyte.motherbrain.flow.error.strategies;

import java.util.concurrent.TimeoutException;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;

import com.zillabyte.motherbrain.flow.operations.LoopException;
import com.zillabyte.motherbrain.flow.operations.Operation;
import com.zillabyte.motherbrain.flow.operations.OperationLogger;
import com.zillabyte.motherbrain.flow.operations.multilang.operations.MultiLangOperation;
import com.zillabyte.motherbrain.reactor.lightweight.ProcessTimeoutException;
import com.zillabyte.motherbrain.universe.Config;
import com.zillabyte.motherbrain.utils.Utils;


/****
 * A forgiving error strategy that will only propagate errors after 50% of loop calls result in an error...
 * @author jake
 *
 */
public class PassiveWorkerPercentageAndAbsoluteOperationErrorStrategy implements OperationErrorStrategy {

  private final boolean THROW_FAKE_EXCEPTIONS = Config.getOrDefault("operation.throw.fake.exceptions", Boolean.FALSE).booleanValue();
  private static Logger _log = Utils.getLogger(PassiveWorkerPercentageAndAbsoluteOperationErrorStrategy.class);
  private Operation _op;
  
  private long _minLoopCalls = Config.getOrDefault("operation.errors.min.loop.calls", 20L);
  private long _maxErrorCount = Config.getOrDefault("operation.errors.max.error.count", 100L);
  private float _maxErrorPercentage = Config.getOrDefault("operation.errors.max.error.percentage", 0.5F);
  private long _loopErrors = 0L;
  
  
  /***
   * 
   * @param op
   */
  public PassiveWorkerPercentageAndAbsoluteOperationErrorStrategy(Operation op) {
    _op = op;
  }
  
  
  @Override
  public synchronized void handleFatalError(Throwable error) throws FakeLocalException {
    
    // Init
    _log.error("fatalError: " + error + " [stacktrace]: " + ExceptionUtils.getFullStackTrace(error));
    
    try {
      
      _op.reportError(error);
      final String state = _op.getState();
      if(!(state.equalsIgnoreCase("KILLING") || state.equalsIgnoreCase("KILLED"))) _op.transitionToState("ERROR", true);
      
    } catch (Exception e) {
      _log.warn("An error occurred when trying to transition to state ERROR: " + e.getMessage());
    }
    
  }
  
  @Override
  public synchronized void handleHeartbeatDeath() {

    try {
      final String state = _op.getState();
      if(!(state.equalsIgnoreCase("KILLING") || state.equalsIgnoreCase("KILLED"))) {
        _log.error("Heartbeat is dead for " + _op.instanceName());
        _op.transitionToState("ERROR", true);
        _op.logger().writeLog("Heartbeat is dead for " + _op.instanceName(), OperationLogger.LogPriority.ERROR);
      }
    } catch (Exception e) {
      _log.warn("An error occurred when trying to transition to state ERROR: " + e.getMessage());
    }
    
  }
  
  
  
  
  /**
   * @throws FakeLocalException 
   * @throws TimeoutException 
   * @throws CoordinationException 
   * @throws StateMachineException 
   * @throws ErrorThresholdExceededException, OperationDeadException **
   * 
   */
  @Override
  public synchronized void handleLoopError(LoopException error) throws FakeLocalException {

    // Init
    _loopErrors++;
    _log.warn("loopError (" + _loopErrors + "): " + error + " [stacktrace]: " + ExceptionUtils.getFullStackTrace(error));
    
    // Some errors we always want to propogate... 
    if (Utils.isCause(error, ProcessTimeoutException.class)) {
      
      // This is taking too long! 
      if (this._op instanceof MultiLangOperation) {
        ((MultiLangOperation)_op).getMultilangHandler().handleRestartingProcess();
      } else {
        _log.warn("Don't know how to handle timeouts on builtin methods!");
      }
      
    }

    // Have we seen enough errors?
    long  loopCalls = _op.getLoopCalls();
    
    // Have we exceeded the percentage?
    if (loopCalls > _minLoopCalls ) {      
      float errorPercentage = (float)_loopErrors / (float)loopCalls;
      if (errorPercentage > _maxErrorPercentage) {
        _op.logger().error("Operation instance "+_op.operationId()+" has exceeded "+_maxErrorPercentage+"% error rate on tuples. If all instances of this operation exceed this threshold, the flow will be shut down.");
        if (_op.inState("SUSPECT", "KILLING", "KILLED", "ERROR", "ERRORING") == false) {
          _op.transitionToState("SUSPECT", true);
        }
      }
    }
      
    // Have we exceeded absolute count? 
    if (_maxErrorCount > 0 && _maxErrorCount < _loopErrors) {
      _op.logger().error("Operation instance "+_op.operationId()+" has exceeded "+_maxErrorCount+" loop errors. If all instances of this operation exceed this threshold, the flow will be shut down.");
      if (_op.inState("SUSPECT", "KILLING", "KILLED", "ERROR", "ERRORING") == false) {
        _op.transitionToState("SUSPECT", true);
      }
    }
    
    // Report the error
    try {
      _op.reportError(error);
    } catch (Exception e) {
      _log.error("ironic error: " + e);
      _loopErrors++;
    }
    
  }



  @Override
  public long getErrorCount() {
    return _loopErrors;
  }
    
    
  
  
}
