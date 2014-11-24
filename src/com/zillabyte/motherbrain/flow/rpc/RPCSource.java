package com.zillabyte.motherbrain.flow.rpc;

import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;
import org.eclipse.jdt.annotation.NonNull;
import org.eclipse.jdt.annotation.NonNullByDefault;
import org.javatuples.Pair;

import com.zillabyte.motherbrain.coordination.CoordinationException;
import com.zillabyte.motherbrain.flow.MapTuple;
import com.zillabyte.motherbrain.flow.StateMachineException;
import com.zillabyte.motherbrain.flow.collectors.OutputCollector;
import com.zillabyte.motherbrain.flow.collectors.coordinated.CoordinatedOutputCollector;
import com.zillabyte.motherbrain.flow.operations.FunctionState;
import com.zillabyte.motherbrain.flow.operations.OperationException;
import com.zillabyte.motherbrain.flow.operations.Source;
import com.zillabyte.motherbrain.flow.operations.SourceState;
import com.zillabyte.motherbrain.flow.operations.multilang.MultiLangException;
import com.zillabyte.motherbrain.flow.rpc.queues.InputQueue;
import com.zillabyte.motherbrain.top.MotherbrainException;
import com.zillabyte.motherbrain.universe.Config;
import com.zillabyte.motherbrain.universe.Universe;
import com.zillabyte.motherbrain.utils.Utils;

@NonNullByDefault
public final class RPCSource extends Source {
  
  private static final long serialVersionUID = -8496379143900675797L;
  private static Logger log = Utils.getLogger(RPCSource.class);
  private InputQueue _inputQueue;
  private final long IDLE_INTERVAL_MS = Config.getOrDefault("rpc.source.idle.interval", 1000L * 60 * 5);
  private long _lastEmit = System.currentTimeMillis();

  
  /***
   * 
   * @param node
   */
  public RPCSource(String name) {
    super(name);
  }

    
  /***
   * 
   */
  @Override
  public void prepare() throws OperationException {
    _inputQueue = Universe.instance().rpcQueueFactory().getInputQueue(this);
    _inputQueue.init();
  }
  
  
  /***
   * 
   */
  @Override
  protected boolean nextTuple(OutputCollector rawCollector) throws OperationException, InterruptedException {
    
    // Get the next request (if any) from the queue...
    RPCRequest request = _inputQueue.getNextRequest();
    
    if(request != null) {
      CoordinatedOutputCollector collector = (CoordinatedOutputCollector) rawCollector;
      log.info("processing rpc batch: " + request.id);
      collector.setCurrentBatch(request.id); // RPC sources should not have sub-batches (always 0)

      try {

        // Emit the tuple the stream...
        for(Pair<String, MapTuple> p : request.getTuples()) {
          MapTuple tuple = p.getValue1();
          markEmit();
          collector.emit(tuple);
        }

      } catch(MotherbrainException e) {

        this.logger().logError(e);   

      } finally {
        
        // Finalize the batch, even if there was an error
        collector.explicitlyCompleteBatch(request.id);
        log.info("rpc batch source-done: " + request.id);
      }
      
    } 
    
    // Only idle (return false) if we haven't seen anything for a while..
    boolean idle = isReadyForIdle();
    return !idle;
  }

  
  /***
   * 
   */
  private void markEmit() {
    _lastEmit = System.currentTimeMillis();
  }
  
  
  /***
   * 
   * @return
   */
  private boolean isReadyForIdle() {
    if (_lastEmit + IDLE_INTERVAL_MS < System.currentTimeMillis()) {
      return true; 
    } else {
      return false;
    }
  }

  @Override
  public void handleIdleDetected() throws InterruptedException, OperationException {
    // Transition the RPC source to idle state from STARTED. The onEndCycle below will take care of this transition from EMITTING to IDLE.
    try {
      if (isReadyForIdle() && _state == SourceState.STARTED ) {
        transitionToState(FunctionState.IDLE.toString(), true);
      }
    } catch (StateMachineException | TimeoutException | CoordinationException e) {
      throw new OperationException(this, e);
    }
  }

  /****
   * 
   * RPC Source PARALLELISM MUST BE 1! Change only with great caution.
   * 
   */
  @Override
  public int getMaxParallelism() {
    return 1;
  }

  
  @Override
  public void onBeginCycle(@NonNull OutputCollector output) throws InterruptedException, OperationException, CoordinationException, StateMachineException, TimeoutException  {
    transitionToState(SourceState.EMITTING.toString(), true);
  }
  
  @Override
  public void onEndCycle(OutputCollector output) throws InterruptedException, OperationException, CoordinationException, StateMachineException, TimeoutException {
    transitionToState(SourceState.IDLE.toString(), true);
    try {
      handleStats_ThreadUnsafe();
    } catch (CoordinationException e) {
      throw new OperationException(this, e);
    }
  }
  
  @Override
  public boolean nextTupleDetected() {
    return _inputQueue.nextRequestAvailable();
  }
  
  @Override
  public void cleanup() throws MultiLangException, OperationException, InterruptedException {
    super.cleanup();
    _inputQueue.shutdown();
  }

}
