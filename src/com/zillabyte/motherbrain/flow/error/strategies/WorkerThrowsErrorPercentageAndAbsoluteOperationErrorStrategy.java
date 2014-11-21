package com.zillabyte.motherbrain.flow.error.strategies;

import java.text.MessageFormat;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;

import com.zillabyte.motherbrain.flow.operations.LoopException;
import com.zillabyte.motherbrain.flow.operations.Operation;
import com.zillabyte.motherbrain.universe.Config;
import com.zillabyte.motherbrain.utils.Utils;


/****
 * A forgiving error strategy that will only propagate errors after 50% of loop calls result in an error...
 * @author jake
 *
 */
public class WorkerThrowsErrorPercentageAndAbsoluteOperationErrorStrategy implements OperationErrorStrategy {

  private final boolean THROW_FAKE_EXCEPTIONS = Config.getOrDefault("operation.throw.fake.exceptions", Boolean.FALSE).booleanValue();
  private Logger _log;
  private Operation _op;
  
  private long _minLoopCalls = Config.getOrDefault("operation.errors.min.loop.calls", 20L);
  private long _maxErrorCount = Config.getOrDefault("operation.errors.max.error.count", 100L);
  private float _maxErrorPercentage = Config.getOrDefault("operation.errors.max.error.percentage", 0.5F);
  private long _loopErrors = 0L;
  
  
  /***
   * 
   * @param op
   */
  public WorkerThrowsErrorPercentageAndAbsoluteOperationErrorStrategy(Operation op) {
    _op = op;
    _log = Utils.getLogger(WorkerThrowsErrorPercentageAndAbsoluteOperationErrorStrategy.class);
  }

  
  @Override
  public synchronized void handleFatalError(Throwable error) throws FakeLocalException {
    
    // Init 
    _log.error("fatalError: " + error + " [stacktrace]: " + ExceptionUtils.getFullStackTrace(error));
    
    try {
      
      _op.reportError(error);
      final String state = _op.getState();
      if(!(state.equalsIgnoreCase("KILLING") || state.equalsIgnoreCase("KILLED"))) _op.transitionToState("ERROR", false);
      
    } catch (Exception e) {
      e.printStackTrace();
    }
    
  }
  
  
  
  
  /**
   * @throws FakeLocalException 
   * @throws ErrorThresholdExceededException, OperationDeadException **
   * 
   */
  @Override
  public synchronized void handleLoopError(LoopException error) throws FakeLocalException {

    // Init
    _loopErrors++;
    _log.error("loopError (" + _loopErrors + "): " + error + " [stacktrace]: " + ExceptionUtils.getFullStackTrace(error));

    // Have we seen enough errors?
    long  loopCalls = _op.getLoopCalls();
    
    // Have we exceeded the percentage?
    if (loopCalls > _minLoopCalls ) {      
      float errorPercentage = (float)_loopErrors / (float)loopCalls;
      if (errorPercentage > _maxErrorPercentage) {
        throw new RuntimeException("Aborting because more than " + MessageFormat.format("{0,number,#.##%}", errorPercentage)  +  " of iterations are producing errors");
      }
    }
      
    // Have we exceeded absolute count? 
    if (_maxErrorCount > 0 && _maxErrorCount < _loopErrors) {
      throw new RuntimeException("Aborting because " + _loopErrors + " errors has exceeded the allowed threshold");
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



  @Override
  public void handleHeartbeatDeath() throws FakeLocalException {
    handleFatalError(new Throwable("heartbeat died!"));
  }
    
    
  
  
}
