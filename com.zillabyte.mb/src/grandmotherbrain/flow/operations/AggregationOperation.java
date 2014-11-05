package grandmotherbrain.flow.operations;

import grandmotherbrain.coordination.CoordinationException;
import grandmotherbrain.flow.Fields;
import grandmotherbrain.flow.FlowStateException;
import grandmotherbrain.flow.MapTuple;
import grandmotherbrain.flow.StateMachineException;
import grandmotherbrain.flow.StateMachineHelper;
import grandmotherbrain.flow.aggregation.AggregationException;
import grandmotherbrain.flow.aggregation.AggregationKey;
import grandmotherbrain.flow.collectors.OutputCollector;
import grandmotherbrain.flow.collectors.coordinated.BatchedTuple;
import grandmotherbrain.flow.collectors.coordinated.CoordTupleOptions;
import grandmotherbrain.flow.collectors.coordinated.CoordinatedOutputCollector;
import grandmotherbrain.flow.config.OperationConfig;
import grandmotherbrain.flow.error.strategies.FakeLocalException;
import grandmotherbrain.flow.operations.multilang.MultiLangException;
import grandmotherbrain.relational.MissingFieldException;
import grandmotherbrain.top.MotherbrainException;
import grandmotherbrain.universe.Config;
import grandmotherbrain.utils.Log4jWrapper;
import grandmotherbrain.utils.Utils;
import grandmotherbrain.utils.backoff.ExponentialBackoffTicker;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.monitoring.runtime.instrumentation.common.com.google.common.collect.Maps;

public abstract class AggregationOperation extends Operation implements ProcessableOperation {

  private static final long serialVersionUID = 1954979246178124528L;
  protected final long PING_SLEEP_INTERVAL = Config.getOrDefault("aggergation.ping.interval", 5000L);
  private Log4jWrapper _log = Log4jWrapper.create(AggregationOperation.class, this);

  protected AggregationState _state = AggregationState.INITIAL;

  protected ExponentialBackoffTicker _innerAggregateLogBackoff = new ExponentialBackoffTicker(1000);
  protected ExponentialBackoffTicker _outerAggregateLogBackoff = new ExponentialBackoffTicker(1000);
  private Map<Object, Boolean> _emitCompleteBatches = new ConcurrentHashMap<Object, Boolean>();
  
  private Map<Object, Integer> _iterationStoreKeys = Maps.newConcurrentMap();
  private Boolean _doNotIdle = Boolean.FALSE;
  
  private Map<String, Long> _startAggregationTime = Maps.newConcurrentMap();
  
  /***
   * 
   * @param name
   */
  public AggregationOperation(String name, OperationConfig config) {
    super(name, config);
  }
  
  
  /***
   * 
   * @param name
   */
  public AggregationOperation(String name) {
    this(name, OperationConfig.createEmpty());
  }

  /**
   * @throws InterruptedException 
   * @throws OperationDeadException 
   * @throws OperationException **
   * 
   */
  public abstract void handleEmit(Object batch, Integer aggKeyStore) throws InterruptedException, OperationException, OperationDeadException;

  /***
   * 
   * @param t
   * @param sourceStream 
   * @throws InterruptedException 
   * @throws MissingFieldException 
   */
  public abstract void handleConsume(Object batch, MapTuple t, String sourceStream, OutputCollector c) throws MotherbrainException, InterruptedException;

  /***
   * 
   * @param tuple
   * @throws MissingFieldException
   */
  protected AggregationKey getKey(Fields fields, MapTuple tuple) throws AggregationException {
    
    // Extract the grouping key... 
    List<Object> list = Lists.newArrayListWithExpectedSize(fields.size());
    for(final String s : fields) {
      if (tuple.containsValueKey(s) == false) {
        throw new AggregationException("The tuple " + tuple.toString() + " does not contain the grouping field: " + s);
      }
      list.add(tuple.get(s));
    }
    AggregationKey key = new AggregationKey(list);
    
    return key;
  }

  public void incrementIterationStoreKey(Object batch) {
    if(!_iterationStoreKeys.containsKey(batch)) {
      _iterationStoreKeys.put(batch, 0);
    } else {
      _iterationStoreKeys.put(batch, _iterationStoreKeys.get(batch) + 1);
    }
    _log.info("INCREMENTING ("+instanceName()+") AGG STORE KEY FOR "+batch+" to "+_iterationStoreKeys.get(batch));
  }
  
  public Integer getIterationStoreKey(Object batch) {
    return _iterationStoreKeys.get(batch);
  }
  
  public String iterationStoreKeyPrefix(Object batch, Integer key) {
    return batch+"_"+key+"_";
  }
  
  public String storeKeyPrefix(Object batch) {
    return iterationStoreKeyPrefix(batch, getIterationStoreKey(batch));
  }
  
  /***
   * 
   * @param key
   * @throws OperationException
   */
  protected MapTuple buildTupleFromKey(Fields fields, AggregationKey key) throws OperationException {
    // Init
    MapTuple t = new MapTuple();
    if (fields.size() != key.groupValueSize()) {
      throw new OperationException(this, "the key size did not match the fields size!");
    }
    for(int i=0;i<key.groupValueSize();i++) {
      t.put(fields.get(i), key.getGroupValue(i));
    }
    // done
    return t;
  }

//  
//  /****
//   * 
//   * @throws InterruptedException
//   * @throws OperationException
//   */
//  protected final void startEmittingNormal() throws InterruptedException, OperationException {
//    synchronized (emitMonitor) {
//      
//      // Already running?
//      if (_emittingFuture != null) {
//        if (!_emittingFuture.isDone()) {
//          _log.warn("emittingFuture is already running");
//          return;
//        } else {
//          throw new OperationException(this, "begin_emit received after the emittingFuture has finished, the current state is " + _state);
//        }
////        try {
////          _emittingFuture.get();
////        } catch (ExecutionException e) {
////          throw new OperationException(this, e);
////        }
//      }
//      
//      // Start the aggregation phase
//      _emittingFuture = Utils.run(new Callable<Void>() {
//
//        @Override
//        public Void call() throws OperationException, FakeLocalException {
//          try {
//            
//            // Transition
//            transitionToState(AggregationState.EMITTING.toString(), true);
//            
//            // Do some emitting...
//            incLoop();
//            markBeginActivity();
//            try {
//              handleEmit(AggregationStore.DEFAULT_BATCH);
//            } finally {
//              markEndActivity();
//            }
//            
//            // Done
//            transitionToState(AggregationState.EMITTING_DONE.toString(), true);
//            return null;
//            
//          } catch(MotherbrainException | TimeoutException ex) {
//            // Report stuff going wrong...
//            handleFatalError(ex);
//            return null;
//          } catch (InterruptedException e) {
//            // Swallow, thread boundary
//            return null;
//          }
//        }
//      });
//    }
//  }

  /***
   * 
   * @param t
   * @param c 
   * @throws FakeLocalException 
   * @throws OperationDeadException 
   */
  public void handleProcess(final MapTuple t, final String sourceStream, final OutputCollector c) throws InterruptedException, OperationException, FakeLocalException {
    try {
      
      switch(_state) {
      case STARTED:
        // tuple received when we were idle or finished starting.. transition to active
        // TRICKLE      
      case SUSPECT:
        // When we're in loop_error, we might as well keep consuming...restarting the instance will likely just produce more loop errors anyway
      case IDLE:
        transitionToState(AggregationState.ACTIVE.toString(), true);
        // TRICKLE         
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
            throw new OperationException(AggregationOperation.this, "The operation is not alive.");
          }
          // process
          c.resetCounter();
          
          // Let the aggregator consume this.. 
          Object batch;
          if (t instanceof BatchedTuple) {
            batch = ((BatchedTuple)t).batchId();
          } else {
            throw new IllegalStateException("expected BatchedTuple");
          }
          
          c.resetCounter();
          // _log.info("consuming batch="+batch + " subBatch="+ subBatch + " tuple:" +t);
          handleConsume(batch, t, sourceStream, c);
            
        } catch(OperationException e) {
          handleLoopError(e);
        } catch(InterruptedException e) {
          // Continue processing...
        } catch(Throwable e) {
          handleFatalError(e);
        } finally {
          markEndActivity();
        }
        
        incEmit(c.getCounter());
        return;
        
      case STARTING:
      case INITIAL:
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
        throw new FlowStateException(AggregationOperation.this, "don't know how to handle state: " + _state);
      }
    } catch (TimeoutException e) {
      handleLoopError(e);
    } catch (MotherbrainException e) {
      handleFatalError(e);
    } catch (InterruptedException e) {
      // do nothing
    } catch(FakeLocalException e) {
      e.printAndWait();
    }
  }
  
  

  /***
   * 
   * @param t
   * @param c
   * @throws InterruptedException 
   * @throws OperationDeadException 
   */
  public void handleProcess(MapTuple t, OutputCollector c) throws InterruptedException, OperationException {
    handleProcess(t, "", c);
  }

  
  /***
   * 
   */
  @Override
  public void handleIdleDetected() throws InterruptedException, OperationException {
    if(_doNotIdle) return;
    try {
      if (_state == AggregationState.STARTED || _state == AggregationState.ACTIVE || _state == AggregationState.SUSPECT) {
        transitionToState(FunctionState.IDLE.toString(), true);
      }
    } catch (StateMachineException | TimeoutException | CoordinationException e) {
      throw new OperationException(this, e);
    }
  }

  
  /***
   * 
   */
  @Override
  public void prePrepare() throws InterruptedException, OperationException {
    try {
      transitionToState(AggregationState.STARTING.toString(), true);
    } catch (StateMachineException | TimeoutException | CoordinationException e) {
      throw new OperationException(this, e);
    }
  }

  
  /***
   * 
   */
  @Override
  public final void postPrepare() throws InterruptedException, OperationException {
    try {
      watchForAggregationCommands();
      transitionToState(AggregationState.STARTED.toString(), true);
    } catch (StateMachineException | TimeoutException | CoordinationException e) {
      throw new OperationException(this, e);
    }
  }

  /**
   * @throws InterruptedException
   * @throws MultiLangException
   */
  protected void aggregationCleanup() throws InterruptedException, MultiLangException {
    /* noop */
  }

  
  /***
   * 
   */
  @Override
  public final void cleanup() throws InterruptedException, OperationException, MultiLangException {
    super.cleanup();
    this.aggregationCleanup();
  }


  
  /***
   * 
   * @throws InterruptedException
   * @throws CoordinationException
   */
  private final void watchForAggregationCommands() throws InterruptedException, CoordinationException {
  }


  
  public final void startAggregationThread(final Object batchId) {
    // Initialize the aggregation store key for this batch and this loop-back. (This initializes the store key to 0)
    incrementIterationStoreKey(batchId);
    _doNotIdle = Boolean.TRUE;
    
    // Run async, as this can take a long time..
    Utils.run(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        try {
          CoordinatedOutputCollector c = (CoordinatedOutputCollector) _collector;

          // On the first iteration, we need to check if all upstream (Sources to us) operations have finished. If so, we can start aggregating. 
          // On subsequent iterations, we only need to check with operations on our loopback path (i.e. from ourself and ourself).
          Set<String> pingOperations = opsFromSourcesToSelf();
          
          _log.info(pingOperations);
          while(pingOperations.size() > 0) {
            // We need to save the current store key since while we are emitting, the operation can also consume new tuples (i.e. from loop-
            // backs). These newly consumed tuples need to be stored under a new store key (see the incrementing below). 
            Integer currentIterationStoreKey = getIterationStoreKey(batchId);
            String currentIterationStorePrefix = iterationStoreKeyPrefix(batchId, currentIterationStoreKey);
            
            // We ping all upstream operations to see whether they are processing any tuples, if not, then we can start aggregating.
            while(true) {
              c.sendPingTuplesUpStream(batchId, pingOperations, "agg_safe", CoordTupleOptions.build().addOpt("agg_store_key", currentIterationStoreKey));
              while(!c.pongRepliesReceived(batchId, pingOperations, "agg_safe", currentIterationStoreKey));
              if(!c.aggregationSafe(batchId, pingOperations, currentIterationStoreKey)) {
                Utils.sleep(PING_SLEEP_INTERVAL);
              } else {
                break;
              }
            }
            
            // Increment the store key for newly consumed tuples.
            incrementIterationStoreKey(batchId);
            if(!_startAggregationTime.containsKey(currentIterationStorePrefix)) _startAggregationTime.put(currentIterationStorePrefix, System.currentTimeMillis());
            
            // Do the emitting, then mark that we are done aggregating.
            handleEmit(batchId, currentIterationStoreKey);
            c.handleChecks();

            // We can also update the pingOperations for the next iteration. If there are any iterations at all, we will only need to check
            // operations on our loop-back path after the first pass-through.
            pingOperations = opsFromSelfToSelf();
            if(pingOperations.size() == 0) {
              // The permission_to_agg ping tells all following operations that this operation is done aggregating and they may begin
              // aggregating (if they are aggregators, if not this command is ignored -- in theory we only need to notify downstream
              // aggregators, but I am too lazy to do that right now). The downstream aggregators will still need to pass the "agg_safe"
              // test (see above) to ensure that upstream eaches and sources also do not have any more tuple activity.
              c.sendPingTuplesDownStream(batchId, opsFromSelfToSinks(), "permission_to_agg", CoordTupleOptions.build());
              break; // The no loop-back case.
            }
            
            // If we're done aggregating, it could be that we don't emit anything else upstream, but our siblings might. They may still be
            // executing and might emit more, so we need to wait for them to be all done this iteration as well before we can determine for
            // sure that they won't send anything else upstream.
            Set<Integer> siblings = c.getTaskOperationMap().get(namespaceName());
            for(Integer task : siblings) {
              if(task == c.getTaskId()) continue; // don't send message to self
              // Ping siblings to let them know we are done. We also send the aggregation start time of this instance since we want to use
              // the EARLIEST start time out of all instances when we determine whether or not tuples are sent back upstream (see 
              // seen_new_tuples below).
              c.sendPingTupleUpStream(batchId, task, "done_agg", CoordTupleOptions.build().addOpt("agg_store_key", currentIterationStoreKey).addOpt("agg_start_time", _startAggregationTime.get(currentIterationStorePrefix)));
            }
            if (hasSiblingInstances()) {
              while(!c.siblingsDoneAggregating(batchId, currentIterationStoreKey)) {
                Utils.sleep(100);
              }
            }
            
            // Now that all of our siblings are done and finished emitting, we can check if anything was emitted back upstream. We do this by
            // checking if upstream operations received anything between the time we started aggregating to now.
            c.sendPingTuplesUpStream(batchId, pingOperations, "seen_new_tuples", CoordTupleOptions.build().addOpt("agg_store_key", currentIterationStoreKey).addOpt("agg_start_time", _startAggregationTime.get(currentIterationStorePrefix)));

            // Wait for responses from all upstream ops...
            while(!c.pongRepliesReceived(batchId, pingOperations, "seen_new_tuples", currentIterationStoreKey));
            if(!c.upStreamReceivedNewTuples(batchId, currentIterationStoreKey)) {
              // No new tuples received upstream...we are done! Send permission_to_agg to all downstream operations.
              c.sendPingTuplesDownStream(batchId, opsFromSelfToSinks(), "permission_to_agg", CoordTupleOptions.build());
              break;
            } else {
              // Received new tuples, we might need to iterate again, so...repeat! Send permission_to_agg through the loop-back path
              // of this operation.
              // NOTE: We MUST explicitly tell subsequent operations that they may begin aggregating because if we do not (i.e. we use
              // the standard "agg_safe" pings/pongs), we may end up in the following race condition: Agg1 finishes emitting, it sets
              // some flag to indicate that it is done emitting, Agg2 is continuously polling Agg1 because it wants to start aggregating
              // when Agg1 is done, however, due to the sleep in the agg_safe check, if Agg1 makes it back to the beginning of the while
              // loop fast enough (where it resets the "I'm done" flag to false), Agg2 might never detect that Agg1 is done.
              // Unfortunately I can't think of a way to get rid of that sleep (i.e. we do have to continuously poll at least the eaches
              // for whether or not they contain tuples as they do not track loop-back "batches" -- there is no way to get an each to 
              // automatically notify an aggregation that it has no more tuples since it does not know when it will have no more tuples).
              c.sendPingTuplesUpStream(batchId, pingOperations, "permission_to_agg", CoordTupleOptions.build());
              continue;
            }

          }

          // Mark the batch as complete once we are sure that upstream operations do not and will not ever have new tuples.
          _emitCompleteBatches.put(batchId, Boolean.TRUE);         
          _doNotIdle = Boolean.FALSE;
          c.maybeCompleteBatch(batchId);
          
        } catch(Exception e) {
          e.printStackTrace();
          Throwables.propagate(e);
        }
        return null;
      }
    });
  }
  
  public void setStartAggTime(Object batchId, Integer aggStoreKey, Long start) {
    synchronized(_startAggregationTime) {
      _log.info(instanceName() + " change agg start time to "+ start);
      _startAggregationTime.put(iterationStoreKeyPrefix(batchId, aggStoreKey), start);
    }
  }
  
  public Long getStartAggTime(Object batchId, Integer aggStoreKey) {
    synchronized(_startAggregationTime) {
      return _startAggregationTime.get(iterationStoreKeyPrefix(batchId, aggStoreKey));
    }
  }
  
  @Override
  public boolean permissionToCompleteBatch(Object batchId) {
    if (_emitCompleteBatches.containsKey(batchId)) {
      return _emitCompleteBatches.get(batchId);
    }
    return false;
  }
  
  

  /***
   * 
   * @param newState
   * @param transactional
   * @throws CoordinationException
   * @throws TimeoutException
   * @throws StateMachineException
   */
  public synchronized void transitionToState(final AggregationState newState, final boolean transactional) throws CoordinationException, TimeoutException, StateMachineException {
    AggregationState oldState = _state;
    _state = StateMachineHelper.transition(_state, newState);
    if(_state != oldState) notifyOfNewState(newState.toString(), transactional);
  }
  
  
  @Override
  public synchronized void transitionToState(String newState, boolean transactional) throws CoordinationException, TimeoutException, StateMachineException {
    transitionToState(AggregationState.valueOf(newState), transactional); 
  }

  @Override
  public String getState() {
    return _state.toString();
  }

  
  @Override
  public String type() {
    return "aggregator";
  }

  
  
}
