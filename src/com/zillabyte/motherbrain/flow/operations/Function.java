package com.zillabyte.motherbrain.flow.operations;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import com.zillabyte.motherbrain.flow.MapTuple;
import com.zillabyte.motherbrain.flow.StateMachineHelper;
import com.zillabyte.motherbrain.flow.collectors.OutputCollector;
import com.zillabyte.motherbrain.flow.config.OperationConfig;
import com.zillabyte.motherbrain.flow.error.strategies.FakeLocalException;
import com.zillabyte.motherbrain.utils.Log4jWrapper;


public abstract class Function extends Operation implements ProcessableOperation {

  private static final long serialVersionUID = 2349633136720169473L;
  protected long _lastProcess;

  protected FunctionState _state = FunctionState.INITIAL;
  private Log4jWrapper _log = Log4jWrapper.create(Function.class, this);


  /***
   * 
   * @param name
   */
  public Function(String name) {
    super(name);
  }
  public Function(String name, OperationConfig config) {
    super(name,config);
  }
  

  /***
   * 
   * @param t
   * @param c
   * @throws InterruptedException 
   * @throws ExecutionException 
   * @throws FakeLocalException 
   */
  protected abstract void process(MapTuple t, OutputCollector c) throws LoopException;

  /***
   * 
   * @param t
   * @param c
   * @throws InterruptedException 
   * @throws FakeLocalException 
   * @throws OperationDeadException 
   */
  public void handleProcess(final MapTuple t, final OutputCollector c) throws FakeLocalException {

    try {

      final FunctionState currentState = _state;

      switch(currentState) {
      case STARTED:
      case IDLE:
        transitionToState(FunctionState.ACTIVE.toString(), true);
        // trickle!
      case PAUSING:
      case SUSPECT:
        // When we're in loop_error, we might as well keep consuming...restarting the instance will likely just produce more loop errors anyway
      case ACTIVE:
        
        heartbeatErrorCheck_ThreadUnsafe();

        // Init
        incLoop();
        incConsumed();
        markBeginActivity();

        try {
          // Logging 
          if (_ipcLogBackoff.tick()) {
            _operationLogger.writeLog("[sampled #" + _ipcLogBackoff.counter() +"] receiving tuple: " + t, OperationLogger.LogPriority.IPC);
          }
          // Make sure we're alive..
          if (isAlive() == false) {
            throw new LoopException(Function.this, "The operation is not alive.");
          }
          // process
          c.resetCounter();
          process(t, c);

        } catch(LoopException e) {
          handleLoopError(e);
        } catch(Exception e) {
          handleFatalError(e);
        } finally {
          markEndActivity();
        }
        incEmit(c.getCounter());
        return;

      case STARTING:
      case INITIAL:
      case PAUSED:

        // All these states, we're just waiting for somebody to change us...
        heartbeatErrorCheck_ThreadUnsafe();
        return;

      case KILLING:
      case KILLED:
      case ERROR:

        // New: Do nothing, this will trigger the flow to kill us.
        return;

      default:
        // This should never be reached.
        throw new RuntimeException("Unknown function state: " + currentState);
      }
    } catch (FakeLocalException e) {
      ((FakeLocalException) e).printAndWait();
    } catch (Exception e) {
      handleFatalError(e);
    }
  }


  @Override
  public void handleIdleDetected() {
    if (_state == FunctionState.PAUSING) {
      transitionToState(SinkState.PAUSED.toString(), true);
    }
    else if (_state == FunctionState.ACTIVE || _state == FunctionState.SUSPECT|| _state == FunctionState.STARTED) {
      transitionToState(FunctionState.IDLE.toString(), true);
    }
  }

  @Override
  public void prePrepare() {
    transitionToState(FunctionState.STARTING.toString(), true);
  }

  @Override
  public void postPrepare() {
    transitionToState(FunctionState.STARTED.toString(), true);
  }

  /***
   * 
   * @throws LoopException
   */
  @Override
  public void handlePause() {
    // Function pause when they IDLE during the PAUSING state
    try {
      if(!getState().equalsIgnoreCase("ERROR")) transitionToState("PAUSING");
    } catch (Exception e) {
      _log.warn("An error occured while trying to resume "+e.getMessage());
    }
  }

  /**
   * @throws LoopException
   */
  @Override
  public void handleResume() {

    // Resume the operation
    try {
      if(!getState().equalsIgnoreCase("ERROR")) transitionToState("ACTIVE");
    } catch (Exception e) {
      _log.warn("An error occured while trying to resume "+e.getMessage());
    }
  }



  @Override
  public String type() {
    return "each";
  }



  /***
   * 
   * @param newState
   * @param transactional
   * @throws CoordinationException
   * @throws TimeoutException
   * @throws StateMachineException
   */
  public synchronized void transitionToState(FunctionState newState, boolean transactional) {
    FunctionState oldState = _state;
    _state = StateMachineHelper.transition(_state, newState);
    if(_state != oldState) notifyOfNewState(newState.toString(), transactional);
  }

  @Override
  public synchronized void transitionToState(String newState, boolean transactional) {
    transitionToState(FunctionState.valueOf(newState), transactional); 
  }

  @Override
  public String getState() {
    return _state.toString();
  }



  //  
  //  /***
  //   * 
  //   */
  //  @Override
  //  public void onThisBatchCompleted(final Object batchId) {
  //    try {
  //      transitionToState("IDLE"); // we 'force' a fast idle because all upstream operations should be done.
  //    } catch (StateMachineException | CoordinationException | TimeoutException e) {
  //      throw new OperationException(this, e);
  //    } 
  //  }


}
