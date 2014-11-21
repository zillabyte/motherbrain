package com.zillabyte.motherbrain.flow.rpc;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import com.google.common.base.Throwables;
import com.zillabyte.motherbrain.flow.MapTuple;
import com.zillabyte.motherbrain.flow.collectors.coordinated.BatchedTuple;
import com.zillabyte.motherbrain.flow.operations.LoopException;
import com.zillabyte.motherbrain.flow.operations.Sink;
import com.zillabyte.motherbrain.flow.rpc.queues.OutputQueue;
import com.zillabyte.motherbrain.universe.Universe;
import com.zillabyte.motherbrain.utils.Utils;


public class RPCSink extends Sink {

  private static final long serialVersionUID = 1621223735806880121L;
  private static Logger log = Utils.getLogger(RPCSink.class);
  
  private OutputQueue _outputQueue;
  private ConcurrentHashMap<Object, LinkedBlockingQueue<MapTuple>> _sinks;
  
  
  /***
   * 
   * @param node
   */
  public RPCSink(String name) {
    super(name);
    _sinks = new ConcurrentHashMap<>();
  }

  /****
   * 
   */
  @Override
  public void prepare() {
    _outputQueue = Universe.instance().rpcQueueFactory().getOutputQueue(this); // appears to be necessary for tests to pass even if already done in constructor
  }
  
  
  /***
   * 
   */
  @Override
  protected void process(MapTuple t) throws LoopException {
    
    // Init 
    log.debug("rpc sinking: " + t);
    if (t instanceof BatchedTuple == false) throw new IllegalStateException();
    BatchedTuple bt = (BatchedTuple) t;

    // Save the tuple for later...
    final Object id = bt.batchId();
    
    if (_sinks.containsKey(id) == false) {
      _sinks.put(id, new LinkedBlockingQueue<MapTuple>());
    } 
    LinkedBlockingQueue<MapTuple> sink = _sinks.get(id);
    try {
      sink.put(t);
    } catch (InterruptedException e) {
      throw new LoopException(this, e);
    }
  }
  
  
  /***
   * 
   */
  @Override
  public void onThisBatchCompleted(Object batchId) {
    
    // Build the response... 
    log.info("batch completed: " + batchId);
    final String id = batchId.toString();
    assert (id != null);
    final RPCResponse response = RPCResponse.create(id);

    final LinkedBlockingQueue<MapTuple> sink = _sinks.get(batchId);
    if (sink != null) {
      final ArrayList<MapTuple> tuples = new ArrayList<>();
      /* Atomically drain any extant responses, but leave the collector around. */
      sink.drainTo(tuples);
      for (final MapTuple tuple : tuples) {
        response.addTuple(this.userGivenName(), tuple);
      }
    }

    try {
      _outputQueue.sendResponse(response);
    } catch (LoopException e) {
      Throwables.propagate(e);
    } 
  }
  
  /****
   * 
   * RPC Sink PARALLELISM MUST BE 1! Change only with great caution.
   * 
   */
  @Override
  public int getMaxParallelism() {
    return 1;
  }
}
