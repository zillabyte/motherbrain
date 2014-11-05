package grandmotherbrain.flow.rpc;

import grandmotherbrain.coordination.CoordinationException;
import grandmotherbrain.flow.MapTuple;
import grandmotherbrain.flow.StateMachineException;
import grandmotherbrain.flow.collectors.OutputCollector;
import grandmotherbrain.flow.collectors.coordinated.CoordinatedOutputCollector;
import grandmotherbrain.flow.operations.FunctionState;
import grandmotherbrain.flow.operations.OperationException;
import grandmotherbrain.flow.operations.Source;
import grandmotherbrain.flow.operations.SourceState;
import grandmotherbrain.flow.operations.multilang.MultiLangException;
import grandmotherbrain.flow.rpc.queues.InputQueue;
import grandmotherbrain.top.MotherbrainException;
import grandmotherbrain.universe.Config;
import grandmotherbrain.universe.Universe;
import grandmotherbrain.utils.Utils;

import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;
import org.eclipse.jdt.annotation.NonNull;
import org.eclipse.jdt.annotation.NonNullByDefault;
import org.javatuples.Pair;

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
  public void prepare() {
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

        // Fail this request 
        log.error(e.getInternalMessage());
        this.logger().error(e.getUserMessage());   

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
