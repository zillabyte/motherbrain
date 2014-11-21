package com.zillabyte.motherbrain.flow.operations;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.zillabyte.motherbrain.flow.StateMachineHelper;
import com.zillabyte.motherbrain.flow.collectors.OutputCollector;
import com.zillabyte.motherbrain.flow.collectors.coordinated.CoordinatedOutputCollector;
import com.zillabyte.motherbrain.flow.config.OperationConfig;
import com.zillabyte.motherbrain.flow.error.strategies.FakeLocalException;
import com.zillabyte.motherbrain.universe.Config;
import com.zillabyte.motherbrain.universe.Universe;
import com.zillabyte.motherbrain.utils.Log4jWrapper;
import com.zillabyte.motherbrain.utils.MeteredLog;
import com.zillabyte.motherbrain.utils.SerializableMonitor;
import com.zillabyte.motherbrain.utils.Utils;

public abstract class Source extends Operation {


  private static final long serialVersionUID = 7524716847415516317L;
  
  private Log4jWrapper _log = Log4jWrapper.create(Source.class, this);


  public static final double PROGRESS_RATE_LIMIT = 0.5;  // Per second

  private static final long PERMISSION_DENIED_WAIT = 1000L * 2;
  private static final long SOURCE_IDLE_WAIT = 1000L * 1;


  private AtomicInteger _cycles = new AtomicInteger(0);
  private double _progress = -1;
  protected SourceState _state = SourceState.INITIAL;
  
  //  private transient Object _stateSem;
  final Serializable prepareMonitor = new SerializableMonitor(); // Used to make sure any initialization in prepare only happens once.

  
  /***
   * 
   * @param operationName
   */
  public Source(String operationName) {
    super(operationName);
  }

  public Source(String name, OperationConfig config) {
    super(name, config);
  }


  @Override
  public int getMaxParallelism() {
    return 1;
  }
  
  
  
  /***
   * 
   */
  @Override
  protected void updateMiscStats(Map<String, Object> stats) {
    super.updateMiscStats(stats);
    if (_progress >= 0) {
      stats.put("progress", _progress);
    }
  }




  /***
   * Asks the FlowInstance for permission to start emitting
   * @throws LoopException 
   * @throws InterruptedException 
   */
  protected boolean askForPermissionToEmit() {
    
    // Init 
    final long timeout = Config.getOrDefault("state.streams.emit_permission.timeout", 1000L * 5);
    final int numTries = Config.getOrDefault("state.streams.emit_permission.tries", 3);
    RetryerBuilder<Boolean> builder = RetryerBuilder.newBuilder();
    
    // Build the retryer
    builder.retryIfExceptionOfType(Exception.class);
    builder.withStopStrategy(StopStrategies.stopAfterAttempt(numTries));
    builder.withWaitStrategy(WaitStrategies.randomWait(2, TimeUnit.SECONDS, 20, TimeUnit.SECONDS));
    Retryer<Boolean> retryer = builder.build();
    
    // Get permission? 
    try {
      return retryer.call(new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          
          /***
           * Ask the FlowInstance for permission to start emitting.  Note that in normal 
           * circumstances, this should always return true or false.  Only when redis/network 
           * issues are afoot will it throw the exception. 
           */
          return (Boolean) Universe.instance().state().ask(_executor, flowStateKey() + "/emit_permission", Boolean.FALSE, timeout);
          
        }
      });
    } catch (ExecutionException e) {
      
      // Some other error... propagate. 
      throw new RuntimeException("An error occurred in the source while it was asking for permission to start emitting.");
      
    } catch (RetryException e) {

      /***
       * If we get a timeout error here, then it is likey because of a redis/network/gamma ray
       * issue. We choose to propagate the exception and kill the operation because we 
       * don't want operations waiting around, thinking they're forever stuck in STARTED. 
       * Instead, just die and let a new operation take its place. 
       */
      throw new RuntimeException("Source operation failed to get a response from master while asking for emit permission.");
    }

  }




  /****
   * Called before the next cycle starts
   * @throws InterruptedException
   * @throws LoopException 
   * @throws TimeoutException 
   * @throws StateMachineException 
   * @throws CoordinationException 
   */
  public void onBeginCycle(OutputCollector output) {
    // We want this transition to be transactional because if we fail to communicate, then 
    // we want this operation to error out; otherwise the flow will hang
    transitionToState(SourceState.EMITTING.toString(), true);
    
    _cycles.incrementAndGet();
    this._progress = -1;
    if (output instanceof CoordinatedOutputCollector) {
      ((CoordinatedOutputCollector)output).setCurrentBatch(Integer.valueOf(this._cycles.get())); // sources will not have sub-batches (always 0)
    }
  }


  /***
   * Called when the cycle ends
   * @throws InterruptedException 
   * @throws LoopException 
   * @throws TimeoutException 
   * @throws StateMachineException 
   * @throws CoordinationException 
   */
  public void onEndCycle(OutputCollector output) {
    //    _sourceLog.info("onEndCycle: " + output);
    transitionToState(SourceState.EMIT_COMPLETE.toString(), true);
    handleStats_ThreadUnsafe();

    if (output instanceof CoordinatedOutputCollector) {
      ((CoordinatedOutputCollector)output).explicitlyCompleteBatch(Utils.valueOf(this._cycles.get()));
    }
  }
  

  /***
   * Returns TRUE if we should continue the cycle, or FALSE if we should end it.
   * @param output
   * @throws InterruptedException 
   */
  protected abstract boolean nextTuple(OutputCollector output) throws LoopException;


  /****
   * 
   * @return
   */
  public int getNumberOfCycles() {
    return _cycles.get();
  }


  /***
   * This is called by the main loop. The purpose of this method is to act like
   * the gate-keeper to the lower 'emitCycle'
   * @param output
   * @throws InterruptedException 
   * @throws LoopException 
   * @throws OperationDeadException 
   */
  public void handleNextTuple(final OutputCollector output) {
    try { 

      if (_sleeper.isShouldSleep()) {
        // System.err.println("sleep");
        heartbeatErrorCheck_ThreadUnsafe();
        return;
      }
      
      final SourceState currentState = _state;
      switch(currentState) {

      case WAITING_FOR_NEXT_CYCLE:
        // fall through
      case STARTED:
        heartbeatErrorCheck_ThreadUnsafe();
        
        // We've finished our prep phase, but haven't received the green light
        // to start processing.  We need permission from FlowInstance. Wait until
        // we get that permission from API (via FlowInstance)
        MeteredLog.info(_log, "asking for permission to start next cycle...(f" + topFlowId() + "-" + instanceName() + ") (" + _state + ")");
        if (askForPermissionToEmit() == false) {
          MeteredLog.info(_log, "permission denied");
          // Wait a bit so we don't hammer the network... 
          _sleeper.sleepFor(PERMISSION_DENIED_WAIT);
          return;
        }
        _log.info("permission granted...(f" + topFlowId() + "-" + instanceName() + ")");
        _log.info("beginning next cycle...(f" + topFlowId() + "-" + instanceName() + ")");
        
        onBeginCycle(output);
        return;

      case SUSPECT:
        // When we're in loop_error, we might as well keep consuming...restarting the instance will likely just produce more loop errors anyway
      case EMITTING:
        
        heartbeatErrorCheck_ThreadUnsafe();
        
        // Ensure we're alive..
        if (isAlive() == false) {
          throw new RuntimeException("The operation is not alive.");
        }

        // Already in EMITTING state, go ahead and emit the next cycle,
        output.resetCounter();
        incLoop();
        markBeginActivity();
        
        boolean continueCycle = true;
        try {
          continueCycle = nextTuple(output);
        } catch(LoopException e) {
          handleLoopError(e);
        } catch(Exception e) {
          handleFatalError(e);
        } finally {          
          markEndActivity();
        }
        
        incEmit(output.getCounter());
        if (continueCycle) {
          // Do nothing... 
        } else {
          onEndCycle(output);
        }

        // Done
        return;

      case IDLE: // only RPC sources should ever be in idle
        heartbeatErrorCheck_ThreadUnsafe();
        if(nextTupleDetected()) {
          onBeginCycle(output);
        } else {
          _sleeper.sleepFor(SOURCE_IDLE_WAIT);
        }
        return;
        
      case EMIT_COMPLETE:
      case EMIT_COMPLETE_ACKED:
      case STARTING:
      case INITIAL:
      case PAUSING:
      case PAUSED:
        
        // All these states, we're just waiting for somebody to change us...
        heartbeatErrorCheck_ThreadUnsafe();
        return; 
        
      case KILLING:
      case KILLED:
      case ERROR:
        
        // New: Do nothing, this will trigger the flow to kill us
        return;
        
      default:
        // This should never be reached.
        throw new RuntimeException("Unknown source state: " + currentState);     
      }
      
    } catch(FakeLocalException e) {
      e.printAndWait();
    } catch (Exception e) {
      handleFatalError(e);
    }
    return; 
  }
  


  /****
   * Called when there's been no activity for a while... 
   */
  @Override
  public void handleIdleDetected() {
    // A source doesn't go into an idle state, so do nothing
  }

  
  /***
   * Sets progress of the source.  Note that this is purely optional, and is really intended
   * just to give the user a sense of things progressing.  Since most sources will actually
   * be SourceFromRelation, the user doesn't need to explicitly call this. 
   * @param p
   */
  public void setProgress(Double p) {
    _progress = Math.min(p.doubleValue(), 1.0);
  }


  public boolean nextTupleDetected() {
    // Only RPC sources should be able to get here, if a normal source gets here.
    return true;
  }


  /***
   * 
   * @return
   * @throws LoopException 
   */
  public boolean isPaused() {
    return (this._state == SourceState.PAUSED);
  }


  
  
  /***
   * 
   * @param command
   * @throws LoopException
   * @throws InterruptedException 
   * @throws CoordinationException 
   */
  @Override
  protected void handleFlowCommand(String command) {
    super.handleFlowCommand(command);
    if (command.equalsIgnoreCase("cycle_acknowledged")) {
      
      // emit_complete has been acknowledged.. transition.. 
      transitionToState(SourceState.WAITING_FOR_NEXT_CYCLE.toString(), true);
      _sleeper.sleepFor(PERMISSION_DENIED_WAIT); // don't be too eager!
      
    }
  }


  /***
   * 
   */
  @Override
  public void prePrepare() {
    transitionToState(SourceState.STARTING.toString(), true);
  }

  
  /***
   * 
   */
  @Override
  public void postPrepare() {
    transitionToState(SourceState.STARTED, true);
  }

  
  /***
   * 
   */
  @Override
  public String type() {
    return "source";
  }

  
  /***
   * 
   */
  public void activate() {

  }

  
  
  /***
   * 
   */
  public void deactivate(){
  }

  /**
   * @param msgId Message ID 
   */
  public void ack(Object msgId) {

  }

  /**
   * @param msgId Message ID 
   */
  public void fail(Object msgId) {

  }

  
  
  /***
   * 
   * @param newState
   * @param transactional
   * @throws CoordinationException
   * @throws TimeoutException
   * @throws StateMachineException
   */
  public synchronized void transitionToState(SourceState newState, boolean transactional) {
    SourceState oldState = _state;
    _state = StateMachineHelper.transition(_state, newState);
    if(_state != oldState) notifyOfNewState(newState.toString(), transactional);
  }
  
  @Override
  public synchronized void transitionToState(String newState, boolean transactional) {
    transitionToState(SourceState.valueOf(newState), transactional); 
  }

  @Override
  public String getState() {
    return _state.toString();
  }

}


