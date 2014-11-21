package com.zillabyte.motherbrain.flow.operations;


import java.util.concurrent.TimeoutException;

import org.eclipse.jdt.annotation.NonNull;

import com.zillabyte.motherbrain.coordination.CoordinationException;
import com.zillabyte.motherbrain.flow.Fields;
import com.zillabyte.motherbrain.flow.FlowStateException;
import com.zillabyte.motherbrain.flow.MapTuple;
import com.zillabyte.motherbrain.flow.StateMachineException;
import com.zillabyte.motherbrain.flow.StateMachineHelper;
import com.zillabyte.motherbrain.flow.collectors.OutputCollector;
import com.zillabyte.motherbrain.flow.error.strategies.FakeLocalException;
import com.zillabyte.motherbrain.top.MotherbrainException;
import com.zillabyte.motherbrain.universe.Universe;
import com.zillabyte.motherbrain.utils.Log4jWrapper;
import com.zillabyte.motherbrain.utils.SerializableMonitor;

public abstract class Sink extends Operation implements ProcessableOperation {
  /**
   * Serialization ID
   */
  private static final long serialVersionUID = 4166297414097397908L;
  final SerializableMonitor prepareMonitor; // Used to make sure any initialization in prepare only happens once.
  protected SinkState _state = SinkState.INITIAL;
  private Fields _columns;


  private Log4jWrapper _log = Log4jWrapper.create(Sink.class, this);

  public Sink(final String id) {
    super(id);
    prepareMonitor = new SerializableMonitor();
  }

  public Fields relationFields() {
    return _columns;
  }

  protected abstract void process(@NonNull MapTuple t) throws OperationException, InterruptedException;
  

  @Override
  public void handleProcess(MapTuple t, OutputCollector c) throws Exception {
    this.handleProcess(t);
  }

  @Override
  public void prePrepare() throws InterruptedException, OperationException {
    try {
      transitionToState(SinkState.STARTING.toString(), true);
    } catch (StateMachineException | TimeoutException | CoordinationException e) {
      throw new OperationException(this, e);
    }
  }
  
  @Override
  public final void postPrepare() throws InterruptedException, OperationException {
    try {
      transitionToState(SinkState.STARTED.toString(), true);
    } catch (StateMachineException | TimeoutException | CoordinationException e) {
      throw new OperationException(this, e);
    }
  }
  
  /***
   * 
   * @param t
   * @throws InterruptedException 
   * @throws OperationException 
   * @throws OperationDeadException 
   */
  public final void handleProcess(final MapTuple t) throws InterruptedException, OperationException, OperationDeadException {
    try {
      
      switch(_state) {
      case STARTED:
      case IDLE:
        // tuple received when we were idle or finished starting.. transition to active
        transitionToState(SinkState.ACTIVE.toString(), true);
        
        // trickle           
      case SUSPECT:
        // When we're in loop_error, we might as well keep consuming...restarting the instance will likely just produce more loop errors anyway
      case PAUSING:
      case ACTIVE:
        heartbeatErrorCheck_ThreadUnsafe();
        
        // Logging 
        if (_ipcLogBackoff.tick() || Universe.instance().env().isLocal()) {
          _operationLogger.writeLog("[sampled #" + _ipcLogBackoff.counter() +"] sinking tuple: " + t, OperationLogger.LogPriority.IPC);
        }

        // Ensure we're alive..
        if (isAlive() == false) {
          throw new OperationDeadException(Sink.this, "The operation is not alive.");
        }

        // process
        incLoop();
        incConsumed();
        markBeginActivity();

        try {
          process(t);
        } finally {
          markEndActivity();
        }
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
        
        // New: Do nothing, this will trigger the flow to kill us
        return;
        
      default:
        
        // This should never be reached.
        throw new FlowStateException(Sink.this, "don't know how to handle state: " + _state);
        
      }
    } catch(InterruptedException e) {
      // Continue processing...
    } catch (TimeoutException | OperationException e) {
      handleLoopError(e);
    } catch (MotherbrainException e) {
      handleFatalError(e);
    } catch(FakeLocalException e) {
      e.printAndWait();
    }
    return;
  }

  
  /***
   * 
   * @throws OperationException
   */
  @Override
  public void handlePause() throws OperationException {
    // Sinks pause when they IDLE during the PAUSING state
    _log.warn("pausing sink");

    try {
      if(!getState().equalsIgnoreCase("ERROR")) transitionToState("PAUSING");
    } catch (StateMachineException | CoordinationException | TimeoutException e) {
      _log.warn("An error occured while trying to resume "+e.getMessage());
    }
  }


  /***
   * 
   * @throws OperationException
   */
  @Override
  public void handleResume() throws OperationException {

    // Resume the operation
    try {
      if(!getState().equalsIgnoreCase("ERROR")) transitionToState("ACTIVE");
    } catch (StateMachineException | CoordinationException | TimeoutException e) {
      _log.warn("An error occured while trying to resume "+e.getMessage());
    }

  }

  /***
   * 
   */
  @Override
  public void handleIdleDetected() throws InterruptedException, OperationException {

    try {
      if (_state == SinkState.PAUSING) {
        transitionToState(SinkState.PAUSED.toString(), true);
      }
      else if (_state == SinkState.ACTIVE || _state == SinkState.SUSPECT || _state == SinkState.STARTED)  {
        transitionToState(SinkState.IDLE.toString(), true);
      }
    } catch (StateMachineException | TimeoutException | CoordinationException e) {
      throw new OperationException(this, e);
    }
  }

  

  @Override
  public final String type() {
    return "sink";
  }

  

  /***
   * 
   * @param newState
   * @param transactional
   * @throws CoordinationException
   * @throws TimeoutException
   * @throws StateMachineException
   */
  public synchronized void transitionToState(SinkState newState, boolean transactional) throws CoordinationException, TimeoutException, StateMachineException {
    SinkState oldState = _state;
    _state = StateMachineHelper.transition(_state, newState);
    if(_state != oldState) notifyOfNewState(newState.toString(), transactional);
  }
  
  @Override
  public synchronized void transitionToState(String newState, boolean transactional) throws CoordinationException, TimeoutException, StateMachineException {
    transitionToState(SinkState.valueOf(newState), transactional); 
  }

  @Override
  public String getState() {
    return _state.toString();
  }


  

  
  
  
}
