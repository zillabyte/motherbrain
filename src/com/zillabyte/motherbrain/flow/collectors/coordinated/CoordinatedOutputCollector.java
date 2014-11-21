package com.zillabyte.motherbrain.flow.collectors.coordinated;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.javatuples.Triplet;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.SetMultimap;
import com.zillabyte.motherbrain.benchmarking.Benchmark;
import com.zillabyte.motherbrain.flow.Flow;
import com.zillabyte.motherbrain.flow.MapTuple;
import com.zillabyte.motherbrain.flow.collectors.OutputCollector;
import com.zillabyte.motherbrain.flow.collectors.coordinated.support.CoordinatedOutputCollectorSupportFactory;
import com.zillabyte.motherbrain.flow.collectors.coordinated.support.FailedTupleHandler;
import com.zillabyte.motherbrain.flow.collectors.coordinated.support.TupleIdGenerator;
import com.zillabyte.motherbrain.flow.collectors.coordinated.support.TupleIdSet;
import com.zillabyte.motherbrain.flow.collectors.coordinated.support.naive.NaiveCoordinatedOutputCollectorSupportFactory;
import com.zillabyte.motherbrain.flow.operations.AggregationOperation;
import com.zillabyte.motherbrain.flow.operations.LoopException;
import com.zillabyte.motherbrain.flow.operations.Operation;
import com.zillabyte.motherbrain.flow.operations.Source;
import com.zillabyte.motherbrain.flow.rpc.RPCHelper;
import com.zillabyte.motherbrain.universe.Config;
import com.zillabyte.motherbrain.utils.Utils;


/*****
 * 
 */
public final class CoordinatedOutputCollector implements OutputCollector {

  

  private static Logger _log = Utils.getLogger(CoordinatedOutputCollector.class);
  
  private OutputCollector _delegate;
  private ConcurrentHashMap<Object, BatchTracker> _batchTrackers = new ConcurrentHashMap<Object, BatchTracker>();
  private Object _currentBatch;
  private Integer _taskId = -1;
  private Operation _operation;
  private boolean _pressureState = false;
  private long _acks = 0;
  private long _fails = 0;
  private long _emits = 0;
  private long _consumes = 0;
  private long _coodinationTuplesEmitted = 0;
  private long _coordinationTuplesReceived = 0;
  private boolean _inProcessing = false;
  
  /***
   * Note on the UNACK_THRESHOLD -- The savvy engineer will note that a high UNACK_THRESHOLD can allow for 
   * downstream operations to run out of memory because they are getting queued up.  To mitigate this, we're
   * using `OffloadableQueue` as the main reactor queue.  This queue will, if necessary, resort to disk or
   * other offline storage if it grows large.  
   */
  

  private final Long UNACK_THRESHOLD = Config.getOrDefault("coordinated.collector.unack.pressure.threshold", 20000L);  // how many unacked tuples do we allow before we start pausing sources?
  
  // TODO: fix this.  The fail-tuple mechanics are currently broken because when an operation gets overloaded with tuples, 
  // the ACKs don't get back in time.  So, instead of failing and ignoring pressure states, we'll just make a hack where we
  // don't fail tuples.   The 'right' solution here is to not fail a tuple unless we get a ping back from the remote operation. 
  private final Long TUPLE_FAIL_MS = Config.getOrDefault("coordinated.collector.tuple.fail.ms", 1000L * 60 * 60 * 5);  // how long to wait before considering a tuple failed?  
  private final Long BATCH_COMPLETE_FAIL_MS = Config.getOrDefault("coordinated.collector.batch_complete.fail.ms", 1000L * 60);  // host long to wait before considering batch-complete signal dead?
  private final long ACK_POOL_MAX_INTERVAL_MS = Config.getOrDefault("coordinated.collector.ack.pool.max.interval.ms", 1000L * 3);  // how long to pool tuple-ids before sending the ack
  
  private int _ackPoolMaxSize = Config.getOrDefault("coordinated.collector.ack.pool.max.size", 1000);  // how many tuples to pool before sending the ack
  
  private final int FAILED_BATCH_ACK_DETECTED_COUNT_LIMIT = Config.getOrDefault("coordinated.collector.failed.batch_ack.detected.count.limit", -1);
  private final int FAILED_TUPLE_ABSOLUTE_LIMIT = Config.getOrDefault("coordinated.collector.failed.tuple.count", -1);
  private final long FAILED_TUPLE_PERCENTAGE_MINIMUM = Config.getOrDefault("coordinated.collector.failed.tuple.percentage.min", -1);
  private final double FAILED_TUPLE_PERCENTAGE = Config.getOrDefault("coordinated.collector.failed.tuple.percentage", 0.5);
  private static final long QUEUE_NOTIFICATION_PERIOD = Config.getOrDefault("coordinated.collector.queued.notification.period", 1000L * 3);
  private static final int SEND_ACKS_AFTER_N_TUPLES_EMITTED = 10;
  
  public static final String COORD_DOWNSTREAM_ID = "ZB_DN";
  public static final String COORD_UPSTREAM_ID = "ZB_UP";

  
  private CoordinatedOutputCollectorSupportFactory _supportFactory;
  private FailedTupleHandler _failedTupleHandler;
  private TupleIdGenerator _tupleIdGenerator;
  private BatchedTuple _currentTuple;
  private Integer _currentSourceTask;
  private Integer _emittedInThisSession = 0;

  private long _lastQueueNotificationSent = 0;
  

  
  


  /****
   * 
   */
  public CoordinatedOutputCollector(OutputCollector collector, CoordinatedOutputCollectorSupportFactory supportFactory) {
    _delegate = collector;
    _operation = _delegate.getOperation();
    _supportFactory = supportFactory;
    _tupleIdGenerator = supportFactory.createTupleIdGenerator(this);
    _failedTupleHandler = supportFactory.createFailedTupleHandler(this);
    
    // if we're an rpc, then have a smaller pool
    if (RPCHelper.isRpcApp(_operation.getTopFlow())) {
      _ackPoolMaxSize = 0;
    }
  }
  
  public CoordinatedOutputCollector(OutputCollector collector) {
    // TODO: swap out with a better supportFactory later on
    this(collector, new NaiveCoordinatedOutputCollectorSupportFactory());
  }
  
  
  public synchronized void setAckPoolMaxSize(int i) {
    _ackPoolMaxSize = i;
  }
  
  public synchronized int getAckPoolMaxSize() {
    return _ackPoolMaxSize;
  }
  
  
  /***
   * 
   */
  @Override
  public synchronized Operation getOperation() {
    return _delegate.getOperation();
  }
  
  
  @Override
  public synchronized void constructTaskOperationInfo(Object context) {
    _delegate.constructTaskOperationInfo(context);
  }
  
  @Override
  public synchronized Set<Integer> getAllTasks() {
    return _delegate.getAllTasks();
  }
  
  @Override
  public synchronized SetMultimap<String, Integer> getTaskOperationMap() {
    return _delegate.getTaskOperationMap();
  }
  
  @Override
  public synchronized Set<Integer> getAdjacentDownStreamTasks() {
    return _delegate.getAdjacentDownStreamTasks();
  }
  
  @Override
  public synchronized Set<Integer> getAdjacentUpStreamNonLoopTasks() {
    return _delegate.getAdjacentUpStreamNonLoopTasks();
  }
  
  /***
   * (re)conigures for new tasks..  
   * @param context
   * @return
   */
  @Override
  public synchronized void configure(Object context) {
    
    Integer newTaskId = _delegate.getThisTask(context);
    
    if (_taskId != newTaskId) {
      _log.warn("_taskId has changed from " + _taskId + " to " + _delegate.getThisTask(context));
      _taskId = newTaskId;
    }
    
    constructTaskOperationInfo(context);
    for(BatchTracker t : this._batchTrackers.values()) {
      Integer tId = t.getTask();
      if(t != null && !getAllTasks().contains(tId)) {
        t.removeDeadTask(tId);
      }
    }
  }
  
  

  private synchronized void maybeSendQueuedNotifications() {

    // Queue notiications are sent periodically, regardless of size
    if (_lastQueueNotificationSent + QUEUE_NOTIFICATION_PERIOD < System.currentTimeMillis()) {
     
      // Init 
      _lastQueueNotificationSent = System.currentTimeMillis();
      for(BatchTracker tracker : this._batchTrackers.values()) {
        for(Entry<Integer, TupleIdSet> e : tracker.getLocallyQueuedTupleIds().entrySet()) {
          
          // INIT  
          Integer task = e.getKey();
          TupleIdSet set = e.getValue();
          Long oldest = set.getOldest();
          int size = set.size();
          
          if (size > 0) {
            emitCoordTupleUp(task, new QueuedTuple(tracker.getBatch(), _taskId, set));
          }

        }
      }
    }
  }

  
  
  


  
  /***
   * 
   */
  private void maybeSendAcks() {
    maybeSendQueuedNotifications();
    for(BatchTracker tracker : this._batchTrackers.values()) {
      
      // Init 
      int localQueue = tracker.getLocallyQueuedTupleSize();
      
      // Processed acks...
      for(Entry<Integer, TupleIdSet> e : tracker.getProcessedTupleIds().entrySet()) {
        
        // INIT  
        Integer task = e.getKey();
        TupleIdSet set = e.getValue();
        Long oldest = set.getOldest();
        int size = set.size();
        
        // If tuples are older than the threshold, then go ahead and flush them out.
        if (size > 0) {
          if (
              (ACK_POOL_MAX_INTERVAL_MS == -1) || 
              (oldest != null && oldest < System.currentTimeMillis() - ACK_POOL_MAX_INTERVAL_MS) || 
              (size >= _ackPoolMaxSize) ||
              (_emittedInThisSession > SEND_ACKS_AFTER_N_TUPLES_EMITTED)
              ) {
            
            // Send the ack
            debug("send ack (threshold exceeded)");
            emitCoordTupleUp(task, new AckTuple(tracker.getBatch(), _taskId, set));
            tracker.removeProcessedTupleIds(task, set);
            
          }
        }
      }
    }
  }
  
  
  
  
  /***
   * 
   * @param batchId
   */
  private void sendAcks(BatchTracker tracker) {
   
    // INIT 
    Map<Integer, TupleIdSet> acks = tracker.getProcessedTupleIds();
    debug("sendAcks (explicit): " + acks);
    
    // Send each source an ack
    for(Entry<Integer, TupleIdSet> e : acks.entrySet()) {
      
      // Tell them that this tuple as acked... 
      if (e.getValue().size() > 0) {
        emitCoordTupleUp(e.getKey(), new AckTuple(tracker.getBatch(), _taskId, e.getValue()));
      }
      
    }
    
    // Remove the received tuples... 
    tracker.removeProcessedTupleIds(acks);
  }
  
  

  /****
   * This should only be called when the component is a source or an aggregator
   */
  public synchronized void setCurrentBatch(Object batchId) {
    this._currentBatch = batchId;
  }




  /****
   * 
   * @param batch
   */
  private final BatchTracker getTrackerForBatch(final Object batch) {
    if (_batchTrackers.containsKey(batch) == false) {
      if(_operation instanceof AggregationOperation) {
        // TODO: this is a performance hole: we do not want to start a new thread for every batch, as that could easily get out of hand.
        // We need to initialize the store key of the aggregation too if the tracker for this batch was just instantiated.
        ((AggregationOperation) _operation).startAggregationThread(batch);
      }
      _batchTrackers.put(batch, new BatchTracker(batch, this));
    }
    return _batchTrackers.get(batch);
  }
  
  
  

  /**
   * Observes tuple before they go into the reactor queue.  This helps with the situation where there is a large
   * back-log on the queue, but we need to process certain messages to even see if downstream opeations are 
   * alive.  
   */
  @Override
  public synchronized ObserveIncomingTupleAction observePreQueuedCoordTuple(Object tuple, Integer originTask) throws LoopException {
    
    debugIPC("observeIncomingPRE: from=" + originTask + " tuple=" + tuple + " totalCoordTupleCount=" + _coordinationTuplesReceived);
    
    if (tuple instanceof QueuedTuple) {
      
      // INIT
      _coordinationTuplesReceived++;
      QueuedTuple queuedTuple = ((QueuedTuple)tuple);
      TupleIdSet set = queuedTuple.tupleIdSet();
      BatchTracker tracker = getTrackerForBatch(queuedTuple.batchId());
      tracker.replaceTuplesRemotelyQueuedAt(set, originTask);
      tracker.resetUnAckedTupleTimestamp(originTask, set);
      
      // no further processing
      return ObserveIncomingTupleAction.STOP;
      
    } if (tuple instanceof BatchedTuple) {
      
      // We've received a new tuple, but it is going on the queue after this.  So, let's mark that it 
      // is currently queued, in case the queue happens to be HUGE.
      Object thisBatch = ((BatchedTuple)tuple).batchId();
      BatchTracker tracker = this.getTrackerForBatch(thisBatch);
      tracker.markTupleLocallyQueuedFrom((BatchedTuple)tuple, originTask);

      // Continue.. 
      return ObserveIncomingTupleAction.CONTINUE;
    }
    
    // Done
    return ObserveIncomingTupleAction.CONTINUE;
  }
  
  
  
  
  
  /**
   * @throws LoopException **
   * 
   */
  @Override
  public synchronized ObserveIncomingTupleAction observePostQueuedCoordTuple(Object tuple, Integer originTask) throws LoopException {
    
    debugIPC("["+_operation.instanceName()+", taskId="+_taskId+"] observeIncomingPOST: from=" + originTask + " tuple=" + tuple + " totalCoordTupleCount=" + _coordinationTuplesReceived);
    _emittedInThisSession = 0;
    
    // Periodic checks... 
    maybeSendQueuedNotifications();
    
    /*********************************
     * FROM DOWNSTREAM
     *********************************/
    
    if (tuple instanceof AckTuple) {
      
      _coordinationTuplesReceived++;
      
      // From Downstream. This is telling us the contained tuple msg'ids have been processed.
      AckTuple ackTuple = ((AckTuple)tuple);
      BatchTracker tracker = getTrackerForBatch(ackTuple.batchId());
      
      debug("USING batch="+tracker.getBatch() + " collector:");
      debug("["+_operation.instanceName()+"] RECEIVED ACK TUPLE "+tuple);
      debug("The current batch state is "+ tracker.getBatchState());
      
      // Remove acked tuples...
      tracker.removeRemotelyQueuedTupleIds(ackTuple.fromTaskId(), ackTuple.tupleIdSet());
      tracker.removeUnAckedTupleIds(ackTuple.fromTaskId(), ackTuple.tupleIdSet());
      debug("removed unacked tuples: " + ackTuple.tupleIdSet());
      _acks += ackTuple.tupleIdSet().size();
      
      // Remove pressure state?
      maybeRemovePressureState();
      
      // Have all tuples been acked? 
      debug("un-acked: "+tracker.getUnAckedTuples());
      // If we're COMPLETING, then maybe check for batch completed..
      if (tracker.getBatchState() == BatchState.COMPLETING) {
        this.maybeCompleteBatch();
      }
      
      // no further processing
      return ObserveIncomingTupleAction.STOP;  
    }
    
    if (tuple instanceof BatchCompleteAckTuple) {
      
      _coordinationTuplesReceived++;
      
      // This is telling us that the batchComplete message has been acked.  This means we can clean up resources..   
      BatchCompleteAckTuple bca = ((BatchCompleteAckTuple)tuple);
      BatchTracker tracker = getTrackerForBatch(bca.batchId());
      
      // Remove acked tuples...
      tracker.markThatDownstreamBatchAckHasBeenReceived(bca.fromTaskId());
      tracker.resetBatchCompleteAttempts(bca.fromTaskId());
      
      // Have all downstreams reported the batch-ack? 
      for (Integer task : getAdjacentDownStreamTasks()) {
        if (tracker.hasDownstreamBatchAckBeenReceived(task) == false) {
          // Ack has not been received... do nothign
          return ObserveIncomingTupleAction.STOP;
        }
      }
      
      // Else, if we get here, then all downstream tasks have been received.  We can now clean up.
      tracker.setBatchState(BatchState.ACKED);
//      handleCleanup(tracker);
      
      // no further processing
      return ObserveIncomingTupleAction.STOP;  
    }
    
    if (tuple instanceof PingTuple) {
      
      _coordinationTuplesReceived++;
      
      // We're being pinged, better respond with a pong.
      PingTuple pingTuple = (PingTuple) tuple;
      BatchTracker tracker = getTrackerForBatch(pingTuple.batchId());
      String message = pingTuple.messsage();
      CoordTupleOptions opts = pingTuple.options();
      debug(_operation.instanceName()+"(" + _taskId+") receiving PING "+pingTuple);

      PongTuple pongTuple = null;
      switch(message) {
      case "agg_safe":
        // query from downstream aggregators asking whether or not _operation is still emitting or is still processing tuples
        // TODO: I'm not sure we actually need to pass in the aggStoreKey here since it's just copied back in the Pong...but the receiver
        // of the Pong (i.e. the originator of this Ping) should know what aggStoreKey it's currently on.
        if(!(_operation instanceof AggregationOperation)) {
          // AggregationOperations give specific permission for downstream operations to start aggregating (see permission_to_agg - a message
          // sent by aggregators to give downstream aggregators permission to start aggregating). Here we only want to check that
          // non-aggregators are no longer emitting/processing tuples.
          
          Boolean sourceEmitting = Boolean.FALSE;
          if(_operation instanceof Source) {
            if(tracker.getBatchState().equals(BatchState.EMITTING) || tracker.getBatchState().equals(BatchState.COMPLETING)) sourceEmitting = Boolean.TRUE;
          }
          debug("[operation: "+_operation.instanceName()+" task: "+_taskId+" batch: "+tracker.getBatch()+"-"+tracker.getBatchState()+"] in processing: "+_inProcessing);
          debug("[operation: "+_operation.instanceName()+" task: "+_taskId+" batch: "+tracker.getBatch()+"-"+tracker.getBatchState()+"] source emitting?: "+sourceEmitting);
          if(!_inProcessing && !sourceEmitting) {
            pongTuple = new PongTuple(tracker.getBatch(), _taskId, message, CoordTupleOptions.build().addOpt(message, Boolean.TRUE).addOpt("agg_store_key", opts.getInt("agg_store_key")));
          } else {
            pongTuple = new PongTuple(tracker.getBatch(), _taskId, message, CoordTupleOptions.build().addOpt(message,  Boolean.FALSE).addOpt("agg_store_key", opts.getInt("agg_store_key")));
          }
          
        }
        break;
      case "permission_to_agg":
        // message from preceding operations (both in loop-back paths or non-loopback paths) that _operation may start aggregating
        if(_operation instanceof AggregationOperation) {
          // Tell the current operation that the originator of this operation has given it permission to aggregate
          AggregationOperation aggOp = (AggregationOperation) _operation;
          tracker.setTaskAggSafe(aggOp.getIterationStoreKey(tracker.getBatch()), pingTuple.fromTaskId(), Boolean.TRUE);
        }       
        break;
      case "done_agg":
        // only called between instances of the same aggregation operation
        // Get the earliest start aggregation time out of all sibling instances.
        Long siblingStartTime = opts.getLong("agg_start_time");
        Long myStartTime = ((AggregationOperation) _operation).getStartAggTime(tracker.getBatch(), opts.getInt("agg_store_key"));
        if(myStartTime == null || myStartTime > siblingStartTime) ((AggregationOperation) _operation).setStartAggTime(tracker.getBatch(), opts.getInt("agg_store_key"), siblingStartTime);
        tracker.setSiblingTaskDoneAgg(opts.getInt("agg_store_key"), pingTuple.fromTaskId());
        break;
      case "seen_new_tuples":
        // message from downstream aggregators asking if _operation has processed any tuples since the given time
        Long baseTime = opts.getLong("agg_start_time");
        Long lastBatchedTime = tracker.getLastBatchedTupleSeenAt();
        if(lastBatchedTime == null || lastBatchedTime - baseTime <= 0) {
          pongTuple = new PongTuple(tracker.getBatch(), _taskId, message, CoordTupleOptions.build().addOpt(message, Boolean.FALSE).addOpt("agg_store_key", opts.getInt("agg_store_key")));
        } else {
          pongTuple = new PongTuple(tracker.getBatch(), _taskId, message, CoordTupleOptions.build().addOpt(message, Boolean.TRUE).addOpt("agg_store_key", opts.getInt("agg_store_key")));
        }
        break;
      default:
        throw new RuntimeException ("undefined message in ping tuple: "+message);
      }
      if(pongTuple != null) sendPongTupleUpStream(pongTuple, pingTuple.fromTaskId());
      return ObserveIncomingTupleAction.STOP;
    }

    /*********************************
     * FROM UPSTREAM
     *********************************/
    if (tuple instanceof PongTuple) {
      _coordinationTuplesReceived++;
      
      PongTuple pongTuple = (PongTuple) tuple;
      BatchTracker tracker = getTrackerForBatch(pongTuple.batchId());
      String reply = pongTuple.reply();
      CoordTupleOptions opts = pongTuple.options();
      
      debug(_operation.instanceName()+"("+_taskId+") receiving PONG "+pongTuple);
      switch(reply) {
      case "agg_safe":
        // reply to agg_safe ping telling _operation if the originating task is still emitting or processing tuples
        tracker.setTaskAggSafe(opts.getInt("agg_store_key"), pongTuple.fromTaskId(), opts.getBoolean(reply));
        break;
      case "seen_new_tuples":
        // reply to seen_new_tuples ping telling _operation if the originating task has seen new tuples since the query time
        tracker.setCheckTuple(opts.getInt("agg_store_key"), pongTuple.fromTaskId(), opts.getBoolean(reply));
        break;
      default:
        throw new RuntimeException ("undefined reply in pong tuple: "+reply);
      }
      return ObserveIncomingTupleAction.STOP;
    }
    
    if (tuple instanceof BatchCompleteTuple) {
      
      _coordinationTuplesReceived++;
      
      // From Upstream. This tuple is telling us the batch is complete
      BatchCompleteTuple bc = ((BatchCompleteTuple)tuple);
      BatchTracker tracker = getTrackerForBatch(bc.batchId());
      Integer fromTask = bc.fromTaskId();
      debug("USING batch="+tracker.getBatch() + " collector:");
      debug("["+_operation.instanceName()+"] RECEIVED BATCH COMPLETE TUPLE "+tuple);
      
      // Mark that we've received a batch-complete from the task.. 
      tracker.markThatBatchCompleteHasBeenReceivedFromUpstreamTask(fromTask);
      
      // Send an ACK to the originating task
      emitCoordTupleUp(fromTask, new BatchCompleteAckTuple(bc.batchId(), _taskId));
      
      // Sanity
      Set<Integer> upStreamNonLoopTasks = getAdjacentUpStreamNonLoopTasks();
      if (upStreamNonLoopTasks.size() == 0) {
        throw new IllegalStateException("no upstream tasks, yet we just received a BatchCompleteTuple");
      }
      // TODO: fix this
//      if (upStreamTasks.contains(originTask) == false) {
//        _log.fatal("we received a BatchCompleteTuple from a task we are not aware of.");
//      }
      
      for(Integer upTask : getAdjacentUpStreamNonLoopTasks()) {

        if (tracker.hasReceivedBatchCompleteFromUpstreamTask(upTask) == false) {
          // Nope, keep waiting..
          debug("will not complete this batch becase we have not received batch-complete from: " + upTask);
          return ObserveIncomingTupleAction.STOP;
        }
        if (tracker.hasProcessedTuplesFrom(upTask)) {
          debug("will not complete this batch becase we still have processed tuples (i.e. unsent acks): " + upTask);
          return ObserveIncomingTupleAction.STOP;
        }
        if (tracker.hasLocallyQueuedTuplesFrom(upTask)) {
          debug("will not complete this batch becase we still have queued tuples from: " + upTask);
          return ObserveIncomingTupleAction.STOP;
        }
      }

      startCompletingBatch(tracker);
      return ObserveIncomingTupleAction.STOP;  
    }
    
    if (tuple instanceof ExplicitAckRequestTuple) {
      
      _coordinationTuplesReceived++;
      
      // From Upstream. This tuple is telling us it is done emitting. This is a good signal to now send some acks. 
      ExplicitAckRequestTuple ed = ((ExplicitAckRequestTuple)tuple);
      BatchTracker tracker = getTrackerForBatch(ed.batchId());
      sendAcks(tracker);

      // no further processing
      return ObserveIncomingTupleAction.STOP;  
    }
    
    if (tuple instanceof BatchedTuple) {
      
      _consumes++;
      _inProcessing = true;
      _currentTuple = (BatchedTuple) tuple;
      _currentBatch = ((BatchedTuple)tuple).batchId();
      _currentSourceTask = originTask;
      BatchTracker tracker = getTrackerForBatch(_currentBatch);
      tracker.setLastBatchedTupleSeenAt();
      tracker.removeLocallyQueuedTupleFrom(_currentTuple, _currentSourceTask);
      _delegate.observeIncomingTuple(_currentTuple);
      return ObserveIncomingTupleAction.CONTINUE;
      
    }
    
    // Else. unknown
    throw new RuntimeException("unknown tuple type: " + tuple);
    
  }

  
  public synchronized Boolean aggregationSafe(Object batch, Set<String> pingOperations, Integer aggStoreKey) {
    Map<Integer, Boolean> taskAggSafe = getTrackerForBatch(batch).getAggregationSafe(aggStoreKey);
    SetMultimap<String, Integer> taskOpMap = getTaskOperationMap();
    for(String op : pingOperations) {
      for(Integer task : taskOpMap.get(op)) {
        Boolean aggSafe = taskAggSafe.get(task);
        if(aggSafe == null || !taskAggSafe.get(task)) return Boolean.FALSE;
      }
    }
    return Boolean.TRUE;
  }
  
  public synchronized Boolean siblingsDoneAggregating(Object batch, Integer aggStoreKey) {
    Set<Integer> siblingsDoneAgg = getTrackerForBatch(batch).getSiblingsDoneAggregation(aggStoreKey);
    if(siblingsDoneAgg == null) return Boolean.FALSE;
    SetMultimap<String, Integer> taskOpMap = getTaskOperationMap();
    for(Integer task : taskOpMap.get(_operation.namespaceName())) {
      if(task == _taskId) continue; // obviously we are done ourselves if we are asking
      if(!siblingsDoneAgg.contains(task)) return Boolean.FALSE;
    }
    return Boolean.TRUE;
  }
  

  public synchronized Boolean upStreamReceivedNewTuples(Object batch, Integer aggStoreKey) {
    Map<Integer, Boolean> taskCheckTuple = getTrackerForBatch(batch).getLastTupleCheck(aggStoreKey);
    SetMultimap<String, Integer> taskOpMap = getTaskOperationMap();
    for(String op : _operation.opsFromSelfToSelf()) {
      for(Integer task : taskOpMap.get(op)) {
        if(taskCheckTuple.get(task)) return Boolean.TRUE;
      }
    }
    return Boolean.FALSE;
  }

  public synchronized Boolean pongRepliesReceived(Object batch, Set<String> pingOperations, String message, Integer aggStoreKey) {
    Map<Integer, Boolean> checkMap;
    switch(message) {
    case "agg_safe":
      checkMap = getTrackerForBatch(batch).getAggregationSafe(aggStoreKey);
      break;
    case "seen_new_tuples":
      checkMap = getTrackerForBatch(batch).getLastTupleCheck(aggStoreKey);
      break;
    default:
      throw new RuntimeException("undefined message \""+message+"\" in replies received");
    }
    
    if(checkMap == null) return Boolean.FALSE;
    for(String op : pingOperations) {
//      _log.info(getTaskOperationMap());
//      _log.info(op+" "+_operation.instanceName()+" "+getTaskOperationMap().get(op));
      for(Integer task : getTaskOperationMap().get(op)) {
        if(!checkMap.containsKey(task)) return Boolean.FALSE;
      }
    }
    return Boolean.TRUE;
  }
  
  /***
   * 
   * @param tracker
   */
  private void handleCleanup(BatchTracker tracker) {
    tracker.cleanup();
    this._batchTrackers.remove(tracker.getBatch());
  }
  

  
  /***
   * 
   * @param tracker
   */
  private void startCompletingBatch(BatchTracker tracker) {
    
    // Sanity
    if (tracker.getBatchState() == BatchState.COMPLETING || tracker.getBatchState() == BatchState.COMPLETED || tracker.getBatchState() == BatchState.ACKED) {
      return;
    }
    if (tracker.getBatchState() != BatchState.EMITTING) {
      throw new IllegalStateException("batch= "+tracker.getBatch() + " " + _operation.instanceName() + " unable to transition to COMPLETING from: " + tracker.getBatchState());
    }
    
    // Invoke the callback... 
    _log.info("["+_operation.instanceName()+"] batch="+tracker.getBatch()+" transitioning to COMPLETING");
    tracker.setBatchState(BatchState.COMPLETING);
    try {
      this._operation.onBatchCompleting(tracker.getBatch());
    } catch (LoopException e) {
      Throwables.propagate(e);
    }
    
    // Tell downstreams to send their acks NOW 
    maybeSendAckRequestsTuples(tracker);
    
    maybeCompleteBatch(tracker);
  }
  
  
  private void maybeCompleteBatch(BatchTracker tracker) {

    // Make sure we only do this once...
    if (tracker.getBatchState() == BatchState.COMPLETED) {
      //_log.warn("Ignored duplicate attempt to complete batch: " + tracker.getBatch() );
      return;
    }
    
    if (tracker.getBatchState() != BatchState.COMPLETING) {
      // Ignore
      debug("Ignoring maybeCompleteBatch because we are in state: " + tracker.getBatchState());
      return;
    }
    
    // Will the operation allow us to complete the batch? 
    if (_operation.permissionToCompleteBatch(tracker.getBatch()) == false) {
      debug("Ignoring maybeCompleteBatch because the operation refused permission");
      return;
    }
    
    // Its possible the above callabck emitted some more stuff.. in that case, we'll exit out 
    // and watch for the acks to come back later...
    debug("unacked tuples: "+tracker.getUnAckedTuples());
    if (!tracker.hasNoUnAckedTuples()) {
      debug("not completing batch because we have unacked tuples.");
      return;
    }
    
    // If we get here, then all tuples have been acked, we can transition to COMPLETE
    _log.info("["+_operation.instanceName()+"] batch="+tracker.getBatch()+" transitioning to COMPLETED");
    tracker.setBatchState(BatchState.COMPLETED);
    _operation.onThisBatchCompleted(tracker.getBatch());
    
    // Send the batch complete tuples
    sendBatchCompleteTuples(tracker);
    
  }
  
  public synchronized void maybeCompleteBatch(Object batch) {
    maybeCompleteBatch(getTrackerForBatch(batch));
  }
  
  private void maybeCompleteBatch() {
    for(BatchTracker t : this._batchTrackers.values()) {
      maybeCompleteBatch(t);
    }
  }
  
  
//  
//  private void completeBatch(BatchTracker tracker) {
//    
//    // Make sure we only do this once...
//    if (tracker.getBatchState() == BatchState.COMPLETED) {
//      _log.warn("Ignored multiple attempt to complete batch: " + tracker.getBatch() );
//      return;
//    }
//    
//    // If we get here, then all upstreams have said this batch is completed. Note that the operation may emit new tuples. 
//    try {
//      _operation.onBatchCompleted(tracker.getBatch());
//    } catch (OperationException e) {
//      Throwables.propagate(e);
//    }
//    
//    // Mark state as completed.  This node will itself send a BatchCompleteTuple when downstream tasks ack all outstanding tuples
//    debug("BATCH COMPLETE: " + tracker.getBatch());
//    tracker.setBatchState(BatchState.COMPLETED);
//    
//    // If no outstanding tuples, then tell downstreams about completed batch
//    if (tracker.hasNoUnAckedTuples()) {
//      // Have all tuples been acked?
//      maybeResendBatchCompleteTuples(tracker);
//    } else {
//      // Otherwise, ask for acks sooner than later..
//      sendEmitDoneTuples(tracker);
//    }
//  }

  
  
  public synchronized void handleChecks(Object context) {
    
    // Has storm changed anything on us? 
    configure(context);
    handleChecks();
  }
  

  public synchronized void handleChecks() {

    // Sanity
    if (_inProcessing) return; 
    
    Benchmark.markBegin("coordinated.collector.handle_checks");
    try {
      
      // Sanity checks..
      debug("handleChecks " + this._operation.instanceName());
      maybeFailUnackedTuples();
      maybeThrowSanityErrors();
      
      // Send queued notifications...
      maybeSendQueuedNotifications();
      
      // Maybe we're done with pressure state?
      maybeRemovePressureState();
      
      // Maybe send acks that we've been pooling up..
      maybeSendAcks(); 
      
      // Maybe complete teh batch..
      maybeCompleteBatch();

      
    } finally {
      Benchmark.markEnd("coordinated.collector.handle_checks");
    }
    
  }

  
  
  private void maybeRemovePressureState() {
    if (_pressureState) {
      if (this.getUnackedCount() == 0) {
        _pressureState = false;
      }
    }
  }
  
  
  
  /****
   * 
   */
  private void maybeFailUnackedTuples() {
    
    // INIT 
    Long cutoffTime = System.currentTimeMillis() - TUPLE_FAIL_MS;
    
    for(BatchTracker tracker : this._batchTrackers.values()) {
      for(Entry<Integer, TupleIdSet> e : tracker.getUnAckedTuples().entrySet()) {
        
        // INIT 
        Integer destTask = e.getKey();
        TupleIdSet set = e.getValue();
        TupleIdSet oldTuples = set.getTupleIdsOlderThan(cutoffTime);
        
        // Are the oldTuples currently on the remote queue?
        TupleIdSet queuedTuples = tracker.getRemoteQueuedTuples(destTask);
        oldTuples.clone().removeAll(queuedTuples);
        TupleIdSet failures = oldTuples; 
        
        if (failures.size() > 0) {
          _fails += failures.size();
          _failedTupleHandler.handleFailedTupleIds(this, tracker.getBatch(), failures);
          tracker.removeUnAckedTupleIds(destTask, failures);
          tracker.removeRemotelyQueuedTupleIds(destTask, failures);
        }
        
      }
    }
  }

  
  
  /***
   * 
   * @param tracker
   */
  private void maybeThrowSanityErrors() {
    for(BatchTracker tracker : _batchTrackers.values()) {
      maybeThrowSanityErrors(tracker);
    }
  }
  
  
  
  /***
   * 
   */
  private void maybeThrowSanityErrors(BatchTracker tracker) {
    for(Integer outTask : getAdjacentDownStreamTasks()) {
      
      // Are all tuples dying?  
      if (tracker.getFailedTupleCount(outTask) > FAILED_TUPLE_ABSOLUTE_LIMIT && FAILED_TUPLE_ABSOLUTE_LIMIT != -1) {
        throw new RuntimeException("The task="+outTask+" appears to be dead. Tuples sent to it have failed more than " + tracker.getFailedTupleCount(outTask) + " times");
      }
      
      // Are a percentage of tuples dying? 
      long num = tracker.getFailedTupleCount(outTask);
      long den = tracker.getSentTupleCount(outTask);
      if (den > FAILED_TUPLE_PERCENTAGE_MINIMUM && FAILED_TUPLE_PERCENTAGE_MINIMUM > 0) {
        double p = num / den;
        if (p > FAILED_TUPLE_PERCENTAGE && FAILED_TUPLE_PERCENTAGE != -1) {
          throw new RuntimeException("The task="+outTask+" appears to be dead. Tuples sent to it have failed more than " + p + "% of the time.");
        }
      }
      
      // Sanity... are we getting any BATCH_COMPLETE acks?  
      int attempts = tracker.getDownstreamBatchCompleteAttempts(outTask);
      if (attempts > FAILED_BATCH_ACK_DETECTED_COUNT_LIMIT && FAILED_BATCH_ACK_DETECTED_COUNT_LIMIT != -1) {
        throw new RuntimeException("The task="+outTask+" appears to be dead. It has not responded to any BATCH_COMPLETE messages after " + attempts + " attempts");
      }
      
    }
  }
  
  
  
  /***
   * 
   * @param tracker 
   * @param force
   */
  private void sendBatchCompleteTuples(BatchTracker tracker) {
    debug("["+_operation.instanceName()+"] sendBatchComplete(): " + getAdjacentDownStreamTasks() + " batch="+tracker.getBatch());
    for(Integer outTask : getAdjacentDownStreamTasks()) {
      
      // Has the batch complete already been acked? 
      if (tracker.hasDownstreamBatchAckBeenReceived(outTask)) {
        continue; 
      }
      
      // Send the batch complete tuples, mark when we sent them, just in case it fails.. 
      Long sent = tracker.getDownstreamBatchCompleteSentAt(outTask);
      if (sent + BATCH_COMPLETE_FAIL_MS < System.currentTimeMillis()) {
        emitCoordTupleDown(outTask, new BatchCompleteTuple(tracker.getBatch(), _taskId));
        tracker.markThatBatchCompleteHasBeenSentToDownstreamTask(outTask, System.currentTimeMillis());
      }
    }
  }
  
  public synchronized void sendPingTuplesUpStream(Object batchId, Set<String> pingOperations, String message, CoordTupleOptions options) {
    for(String op : pingOperations) {
      for(Integer task : getTaskOperationMap().get(op)) {
        sendPingTupleUpStream(batchId, task, message, options);
      }
    }
  }
  
  public synchronized void sendPingTupleUpStream(Object batchId, Integer outTask, String message, CoordTupleOptions options) {
    emitCoordTupleUp(outTask, new PingTuple(batchId, _taskId, message, options));
  }
  
  public synchronized void sendPingTuplesDownStream(Object batchId, Set<String> pingOperations, String message, CoordTupleOptions options) {
    for(String op : pingOperations) {
      for(Integer task : getTaskOperationMap().get(op)) {
        sendPingTupleDownStream(batchId, task, message, options);
      }
    }
  }
  
  public synchronized void sendPingTupleDownStream(Object batchId, Integer outTask, String message, CoordTupleOptions options) {
    emitCoordTupleDown(outTask, new PingTuple(batchId, _taskId, message, options));
  }
  
  private void sendPongTupleUpStream(PongTuple pongTuple, Integer outTask) {
    emitCoordTupleDown(outTask, pongTuple);
  }
  
  /***
   * 
   */
  @Override
  public synchronized void emit(String streamName, MapTuple tuple) throws LoopException {
    emitImpl(_operation.prefixifyStreamName(streamName), tuple);
  }
  

  /***
   * 
   */
  @Override
  public synchronized void emit(MapTuple t) throws LoopException {
    this.emitImpl(_delegate.getDefaultStream(), t);
  }
  
  
  
  /***
   * 
   */
  private synchronized void emitImpl(String streamName, MapTuple tuple) throws LoopException {
    
    Benchmark.markBegin("coordinated.collector.emit");
    try {
        
      // INIT
      _emits++;
      _emittedInThisSession++;
      debugIPC("emit (" + streamName + "): " + tuple);
      Object batch = this._currentBatch;

      if (batch == null) throw new IllegalStateException("batch has not been set!");
      BatchTracker tracker = getTrackerForBatch(batch);
      
      // Sanity 
      if (_pressureState) {
         //_log.warn("operation " + _operation.instanceName() + " continues to emit, even though we are in a pressure state!");
      }
      maybeThrowSanityErrors(tracker);
  
      // Emit the tuple, as normal.  The delegate will return a list of tasks that the tuple has been sent to.
      BatchedTuple bt = new BatchedTuple(tuple, _tupleIdGenerator.getTupleIdFor(tuple), batch);
      List<Integer> tasks = _delegate.emitAndGetTasks(streamName, bt);
      
      for(Integer task : tasks) {
        tracker.addUnAckedTupleSentTo(bt, task);
        tracker.incSentTupleCount(task);
      }
      
      // Does this put us into a pressure state?
      if (this.isAtUnackedThreshold()) {
        
        // Enter pressure state..
        _pressureState = true;
        
        // Tell all downstreams to send their acks..
        maybeSendAckRequestsTuples();
      }
      
    } finally {
      Benchmark.markEnd("coordinated.collector.emit");
    }
  }




  
  /***
   * 
   * @param tracker
   */
  private void maybeSendAckRequestsTuples(BatchTracker tracker) {
    // We send the ExplicitAckRequestTuple to tell the operaitons to send their acks sooner than later. 
    for(Integer outTask : getAdjacentDownStreamTasks()) {
      // Is the remote operaiton queue=0? 
      if (tracker.getRemoteQueuedTuples(outTask).size() == 0 && tracker.getLocallyQueuedTupleSize() == 0) {
        emitCoordTupleDown(outTask, new ExplicitAckRequestTuple(tracker.getBatch(), _taskId));
      }
    }
  }
  
  private void maybeSendAckRequestsTuples() {
    for(BatchTracker tracker : this._batchTrackers.values()) {
      maybeSendAckRequestsTuples(tracker);
    }
  }
  

  /***
   * 
   * @param string
   */
  public synchronized void explicitlyCompleteBatch(Object batch) {
    // Init 
    debug("batch completed (explicitly) batch=" + batch);
    startCompletingBatch(getTrackerForBatch(batch));
  }
  

  /***
   * 
   */
  @Override
  public synchronized void observeIncomingTuple(MapTuple tuple) {
    if (tuple instanceof BatchedTuple == false) throw new IllegalStateException("expected batched tuple");
  }


  /***
   * 
   */
  @Override
  public synchronized void emitDirect(Integer taskId, String streamId, List<?> rawTuple) {
    // TODO: we can handle emitDirects, but we need to use other streamIds
    throw new IllegalStateException("you should not call emitDirect() with a CoordinateOutputCollector, otherwise coordination will fail");
  }


  /***
   * 
   */
  @Override
  public synchronized List<Integer> emitAndGetTasks(String streamName, MapTuple t) {
    throw new IllegalStateException("you should not call emitAndGetTasks() with a CoordinateOutputCollector, otherwise coordination will fail");
  }


  /***
   * 
   */
  @Override
  public synchronized Integer getThisTask(Object context) {
    return _delegate.getThisTask(context);
  }

  
  /***
   * 
   */
  @Override
  public synchronized String getDefaultStream() {
    return _delegate.getDefaultStream();
  }

 
  /***
   * 
   */
  @Override
  public synchronized void resetCounter() {
    _delegate.resetCounter();
  }


  /***
   * 
   */
  @Override
  public synchronized long getCounter() {
    return _delegate.getCounter();
  }

  
  /**
   * @throws LoopException *
   * 
   */
  @Override
  public synchronized void onAfterTuplesEmitted() throws LoopException {
    debug("onAfterTuplesEmitted");
    
    BatchTracker tracker = getTrackerForBatch(_currentBatch);
    _inProcessing = false;
    tracker.markTupleProcessedFrom(_currentTuple, _currentSourceTask);
    
    maybeSendAcks();
    _delegate.onAfterTuplesEmitted();
  }

  
  /***
   *   
   * @param fromTask
   * @param coordTuple
   */
  private void emitCoordTupleDown(Integer task, BaseCoordTuple coordTuple) {
    _coodinationTuplesEmitted++;
    debug("coord: task=" + task + " tuple=" + coordTuple);
    handleEmitDirect(task, COORD_DOWNSTREAM_ID, coordTuple);
  }
  
  private void emitCoordTupleUp(Integer task, BaseCoordTuple coordTuple) {
    _coodinationTuplesEmitted++;
    debug("coord: task=" + task + " tuple=" + coordTuple);
    handleEmitDirect(task, COORD_UPSTREAM_ID, coordTuple);
  }

  private void handleEmitDirect(Integer task, String stream, BaseCoordTuple coordTuple) {
    Benchmark.markBegin("coordinated.collector.emit_direct");
    try {
      _delegate.emitDirect(task, stream, Lists.newArrayList(coordTuple));
    } finally {
      Benchmark.markEnd("coordinated.collector.emit_direct");
    }
  }
  

  private void debugTrackers() {
    synchronized(CoordinatedOutputCollector.class) {
      for(BatchTracker t : this._batchTrackers.values()) {
        debug(t.toString());
      }
    }
  }
  
  

  /***
   * 
   * @param flow
   * @return true if the flow should use the coordinated framework
   */
  public static final boolean shouldUse(Flow flow) {
    if (Config.getOrDefault("storm.coordinated", Boolean.TRUE).booleanValue()) {
      return true;
    }
    if (flow.isCoordinated()) {
      return true;
    }
    return false;
  }


  


  public synchronized CoordinatedOutputCollectorSupportFactory getSupportFactory() {
    return this._supportFactory;
  }

  
  public synchronized boolean isAtUnackedThreshold() {
    return getUnackedCount() > UNACK_THRESHOLD;
  }
  
  private synchronized long getUnackedCount() {
    long size = 0;
    for(BatchTracker tracker : this._batchTrackers.values()) {
      for(TupleIdSet set : tracker.getUnAckedTuples().values()) {
        size += set.size();
      }
    }
    return size;
  }
  

  public synchronized Triplet<Integer,Integer,Integer> getUnackedAndLocalQueueAndRemoteQueeuCount_ThreadUnsafe() {
    int unackedSize = 0;
    int localQueueSize= 0;
    int remoteQueueSize= 0;
    for(BatchTracker tracker : this._batchTrackers.values()) {
      for(TupleIdSet set : tracker.getUnAckedTuples().values()) {
        unackedSize += set.size();
      }
      for(TupleIdSet set : tracker.getLocallyQueuedTupleIds().values()) {
        localQueueSize += set.size();
      }
      for(TupleIdSet set : tracker.getRemotelyQueuedTupleIds().values()) {
        remoteQueueSize += set.size();
      }
    }
    return new Triplet<Integer,Integer,Integer>(unackedSize, localQueueSize, remoteQueueSize);
  }
  

  public synchronized Integer getTaskId() {
    return this._taskId;
  }

  
  /***
   * 
   * @return
   */
  public synchronized boolean inPressureState() {
    return _pressureState;
  }

  public synchronized long getFailCount() {
    return this._fails;
  }
  
  public synchronized long getAckCount() {
    return this._acks;
  }
  
  public synchronized long getEmitCount() {
    return this._emits;
  }
  
  public synchronized long getConsumeCount() {
    return this._consumes;
  }
  
  public synchronized long getCoordConsumeCount() {
    return _coordinationTuplesReceived;
  }

  public synchronized long getCoordEmitCount() {
    return _coodinationTuplesEmitted;
  }

  

  /***
   * 
   * @param string
   */
  public void debug(Object m) {
//    System.err.println(this._operation.instanceName()+" (task " + this._taskId + "-"+this._operation.type()+"): " + m);
//    _log.info(this._operation.instanceName()+" (task " + this._taskId + "): " + m);
//    _log.debug(string);
  }
  
  private void debugIPC(Object m) {
    debug(m);
  }

  public synchronized ConcurrentHashMap<Object, BatchTracker> getTrackers() {
    return this._batchTrackers;
  }
  
  @Override
  public Object getCurrentBatch() {
    return _currentBatch;
  }
  
}

