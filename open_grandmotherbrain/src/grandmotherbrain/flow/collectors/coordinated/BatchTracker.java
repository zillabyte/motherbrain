package grandmotherbrain.flow.collectors.coordinated;

import grandmotherbrain.flow.collectors.coordinated.support.CoordinatedOutputCollectorSupportFactory;
import grandmotherbrain.flow.collectors.coordinated.support.TupleIdSet;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.commons.lang.mutable.MutableLong;
import org.javatuples.Pair;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/***
 * 
 * @author jake
 */
public final class BatchTracker {
    
  
  private Object _batchId;
  private BatchState _batchState;
  private CoordinatedOutputCollector _collector;
  private CoordinatedOutputCollectorSupportFactory _supportFactory;
  
  private Map<Integer, TupleIdSet> _unAckedTuples = Maps.newHashMap();
  private Map<Integer, TupleIdSet> _processedTupleIds = Maps.newHashMap();
  
  private Map<Integer, TupleIdSet> _localQueuedTupleIds = Maps.newHashMap();
  private Map<Integer, TupleIdSet> _remoteQueuedTupleIds = Maps.newHashMap();
  
  private Map<Integer, Long> _downstreamBatchAcksReceived = Maps.newHashMap();
  
  private Map<Integer, Long> _upstreamBatchCompletesReceived = Maps.newHashMap();
  private Map<Integer, Pair<MutableInt, MutableLong>> _downstreamBatchCompletesSent = Maps.newHashMap();
  
  private Map<Integer, MutableLong> _failedTupleCounts = Maps.newHashMap();
  private Map<Integer, MutableLong> _sentTupleCounts = Maps.newHashMap();
  
  private Map<Integer, Map<Integer, Boolean>> _aggSafe = Maps.newConcurrentMap();
  private Map<Integer, Set<Integer>> _siblingsDoneAgg = Maps.newConcurrentMap();
  private Map<Integer, Map<Integer, Boolean>> _checkTuple = Maps.newConcurrentMap();
  private Long _lastBatchedTupleSeenAt;

  
  public BatchTracker(Object batchId, CoordinatedOutputCollector collector) {
    if (batchId instanceof BatchTracker) throw new IllegalStateException();
    _batchId = batchId;
    _batchState = BatchState.EMITTING;
    _collector = collector;
  }
  
  
  ///////////////
  // BATCH STATE
  ///////////////

  public BatchState getBatchState() {
    return _batchState;
  }
  
  public void setBatchState(BatchState bs) {
    _batchState = bs;
  }
  
  
  ///////////////
  // TUPLE ACKS
  ///////////////
  

  public void addUnAckedTupleSentTo(BatchedTuple tuple, Integer task) {
    getTupleIdSet(_unAckedTuples, task).add(tuple.getId());
  }

  
  public void removeUnAckedTupleIds(Integer fromTaskId, TupleIdSet set) {
    getTupleIdSet(_unAckedTuples, fromTaskId).removeAll(set);
  }
  
  public void resetUnAckedTupleTimestamp(Integer fromTaskId, TupleIdSet set) {
    getTupleIdSet(_unAckedTuples, fromTaskId).setTimestamp(set, System.currentTimeMillis());
  }
  
  public boolean hasNoUnAckedTuples() {
    for(Entry<Integer, TupleIdSet> e : _unAckedTuples.entrySet()) {
      if (e.getValue().size() > 0) {
        return false;
      }
    }
    return true;
  }

  public Map<Integer, TupleIdSet> getUnAckedTuples() {
    return _unAckedTuples;
  }
  
  
  
  
  
  
  ///////////////
  // BATCH ACKS
  ///////////////
  
  public void markThatDownstreamBatchAckHasBeenReceived(Integer fromTaskId) {
    _downstreamBatchAcksReceived.put(fromTaskId, System.currentTimeMillis());
  }
  
  public boolean hasDownstreamBatchAckBeenReceived(Integer outTask) {
    return _downstreamBatchAcksReceived.containsKey(outTask);
  }
  
  
  
  ///////////////
  // BATCH COMPLETES - UPSTREAM
  ///////////////

  public void markThatBatchCompleteHasBeenReceivedFromUpstreamTask(Integer fromTask) {
    _upstreamBatchCompletesReceived.put(fromTask, System.currentTimeMillis());
  }
  

  public boolean hasReceivedBatchCompleteFromUpstreamTask(Integer fromTask) {
    return _upstreamBatchCompletesReceived.containsKey(fromTask);
  }

  
  
  
  ///////////////
  // BATCH COMPLETES - DOWNSTREAM
  ///////////////

  public Long getDownstreamBatchCompleteSentAt(Integer outTask) {
    if (_downstreamBatchCompletesSent.containsKey(outTask)) {
      return _downstreamBatchCompletesSent.get(outTask).getValue1().longValue();
    } else {
      return 0L;
    }
  }
 
  public void resetBatchCompleteAttempts(Integer outTask) {
    if (_downstreamBatchCompletesSent.containsKey(outTask)) {
      _downstreamBatchCompletesSent.get(outTask).getValue0().setValue(0);
    }
  }
  
  public Integer getDownstreamBatchCompleteAttempts(Integer outTask) {
    if (_downstreamBatchCompletesSent.containsKey(outTask)) {
      return _downstreamBatchCompletesSent.get(outTask).getValue0().intValue();
    } else {
      return 0;
    }
  }

  public void markThatBatchCompleteHasBeenSentToDownstreamTask(Integer outTask, Long time) {
    if (_downstreamBatchCompletesSent.containsKey(outTask) == false) {
      _downstreamBatchCompletesSent.put(outTask, new Pair<MutableInt, MutableLong>(new MutableInt(0), new MutableLong(0)));
    }
    Pair<MutableInt, MutableLong> p = _downstreamBatchCompletesSent.get(outTask);
    p.getValue0().increment();
    p.getValue1().setValue(time);
  }
  
  

  
  ///////////////
  // PROCESSED TUPLES
  ///////////////
  
  public Map<Integer, TupleIdSet> getProcessedTupleIds() {
    return _processedTupleIds;
  }
  
  public void markTupleProcessedFrom(BatchedTuple tuple, Integer originTask) {
    getTupleIdSet(_processedTupleIds, originTask).add(tuple.getId());
  }
  
  public boolean hasProcessedTuplesFrom(Integer originTask) {
    return getTupleIdSet(_processedTupleIds, originTask).size() > 0;
  }

  public void removeProcessedTupleIds(Integer task, TupleIdSet acks) {
    getTupleIdSet(_processedTupleIds, task).removeAll(acks);
  }

  public void removeProcessedTupleIds(Map<Integer, TupleIdSet> acks) {
    for(Entry<Integer, TupleIdSet> e : acks.entrySet()) {
      removeProcessedTupleIds(e.getKey(), e.getValue());
    }
  }
  
  
  

  
  ///////////////
  // QUEUED TUPLES - LOCAL
  ///////////////


  public Map<Integer, TupleIdSet> getLocallyQueuedTupleIds() {
    return this._localQueuedTupleIds;
  }
  

  public int getLocallyQueuedTupleSize() {
    int size = 0;
    for(Entry<Integer, TupleIdSet> e : _localQueuedTupleIds.entrySet()) {
      size += e.getValue().size();
    }
    return size;
  }



  public void markTupleLocallyQueuedFrom(BatchedTuple tuple, Integer originTask) {
    getTupleIdSet(_localQueuedTupleIds, originTask).add(tuple.getId());
  }
  
  public void removeLocallyQueuedTupleFrom(BatchedTuple tuple, Integer task) {
    getTupleIdSet(_localQueuedTupleIds, task).remove(tuple.getId());
  }
  
  public boolean hasLocallyQueuedTuplesFrom(Integer task) {
    return getTupleIdSet(_localQueuedTupleIds, task).size() > 0;
  }


  

  ///////////////
  // QUEUED TUPLES - REMOTE
  ///////////////
  
  public void replaceTuplesRemotelyQueuedAt(TupleIdSet set, Integer originTask) {
    getTupleIdSet(_remoteQueuedTupleIds, originTask).clear();
    getTupleIdSet(_remoteQueuedTupleIds, originTask).addAll(set);
  }

  public void removeRemotelyQueuedTupleIds(Integer task, TupleIdSet set) {
    getTupleIdSet(_remoteQueuedTupleIds, task).removeAll(set);
  }

  public TupleIdSet getRemoteQueuedTuples(Integer task) {
    return getTupleIdSet(_remoteQueuedTupleIds, task);
  }
  
  public Map<Integer, TupleIdSet> getRemotelyQueuedTupleIds() {
    return this._remoteQueuedTupleIds;
  }
  
  

  ///////////////
  // HELPERS
  ///////////////

  public void setCurrentFailedTupleCount(Integer task, int size) {
    if (_failedTupleCounts .containsKey(task) == false) {
      _failedTupleCounts.put(task, new MutableLong());
    }
    _failedTupleCounts.get(task).setValue(size);
  }
  
  public long getFailedTupleCount(Integer task) {
    if (_failedTupleCounts.containsKey(task)) {
      return _failedTupleCounts.get(task).longValue();
    } else {
      return 0;
    }
  }
  
  
  public synchronized void setTaskAggSafe(Integer aggStoreKey, Integer task, Boolean state) {
    Map<Integer, Boolean> aggSafe;
    if(_aggSafe.containsKey(aggStoreKey)) {
      aggSafe = _aggSafe.get(aggStoreKey); 
    } else {
      aggSafe = Maps.newConcurrentMap();
    }
    aggSafe.put(task, state);
    _aggSafe.put(aggStoreKey, aggSafe);
  }
  
  public synchronized void setSiblingTaskDoneAgg(Integer aggStoreKey, Integer task) {
    Set<Integer> doneSiblings;
    if(_siblingsDoneAgg.containsKey(aggStoreKey)) {
      doneSiblings = _siblingsDoneAgg.get(aggStoreKey);
      doneSiblings.add(task);
    } else {
      doneSiblings = Sets.newHashSet(task);
    }
    _siblingsDoneAgg.put(aggStoreKey, doneSiblings);
  }
  
  public synchronized void setCheckTuple(Integer aggStoreKey, Integer task, Boolean state) {
    Map<Integer, Boolean> checkTuple;
    if(_checkTuple.containsKey(aggStoreKey)) {
      checkTuple = _checkTuple.get(aggStoreKey); 
    } else {
      checkTuple = Maps.newConcurrentMap();
    }
    checkTuple.put(task, state);
    _checkTuple.put(aggStoreKey, checkTuple);
  }

  public Map<Integer, Boolean> getAggregationSafe(Integer aggStoreKey) {
    return _aggSafe.get(aggStoreKey);
  }
  
  public Set<Integer> getSiblingsDoneAggregation(Integer aggStoreKey) {
    return _siblingsDoneAgg.get(aggStoreKey);
  }
  
  public Map<Integer, Boolean> getLastTupleCheck(Integer aggStoreKey) {
    return _checkTuple.get(aggStoreKey);
  }
  
  public void incSentTupleCount(Integer task) {
    if (_sentTupleCounts .containsKey(task) == false) {
      _sentTupleCounts.put(task, new MutableLong());
    }
    _sentTupleCounts.get(task).increment();
  }
  
  public long getSentTupleCount(Integer task) {
    if (_sentTupleCounts.containsKey(task)) {
      return _sentTupleCounts.get(task).longValue();
    } else {
      return 0;
    }
  }
  



  private TupleIdSet getTupleIdSet(Map<Integer, TupleIdSet> map, Integer task) {
    if (map.containsKey(task) == false) {
      map.put(task, _collector.getSupportFactory().createTupleIdSet(_collector));
    }
    return map.get(task);
  }


  public Object getBatch() {
    return this._batchId;
  }

  public Integer getTask() {
    return this._collector.getTaskId();
  }

  public void removeDeadTask(Integer task) {
    // TODO Auto-generated method stub
    throw new NotImplementedException();
    
  }
  
  public void setLastBatchedTupleSeenAt() {
    _lastBatchedTupleSeenAt = System.currentTimeMillis();
  }
  
  public Long getLastBatchedTupleSeenAt() {
    return _lastBatchedTupleSeenAt;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("\n");
    sb.append("**********************************");sb.append("\n");
    sb.append("* batch: " + _batchId);sb.append("\n");
    sb.append("* state: " + _batchState);sb.append("\n");
    sb.append("* unacked: " + _unAckedTuples);sb.append("\n");
    sb.append("* remote-queued: " + _remoteQueuedTupleIds);sb.append("\n");
    sb.append("* local-queued: " + _localQueuedTupleIds);sb.append("\n");
    sb.append("* processed: " + _processedTupleIds);sb.append("\n");
    sb.append("*- - - - - - - -");sb.append("\n");
    sb.append("* upstream_batch-completes_received: " + _upstreamBatchCompletesReceived);sb.append("\n");
    sb.append("* downstream_batch-acks_received: " + _downstreamBatchAcksReceived);sb.append("\n");
    sb.append("* downstream_batch-completes_sent: " + _downstreamBatchCompletesSent);sb.append("\n");
    sb.append("**********************************");sb.append("\n");
    return sb.toString();
  }


  
  
  public void cleanup() {
    
  }





 

  

   
  
}