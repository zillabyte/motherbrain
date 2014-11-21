package com.zillabyte.motherbrain.flow.aggregation;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Maps;
import com.google.monitoring.runtime.instrumentation.common.com.google.common.collect.Sets;
import com.zillabyte.motherbrain.flow.MapTuple;
import com.zillabyte.motherbrain.flow.operations.LoopException;

/***
 * Use this for testing, not for production
 */
public class MemoryStore implements AggregationStore {

  private static final long serialVersionUID = -1837276500478667648L;
  
  private Map<Object, LinkedListMultimap<AggregationKey, MapTuple>> _map = Maps.newHashMap(); 

  
  private LinkedListMultimap<AggregationKey, MapTuple> getMap(Object batch) {
    if (_map.containsKey(batch) == false) {
      LinkedListMultimap<AggregationKey, MapTuple> innerMap = LinkedListMultimap.create(); 
      _map.put(batch, innerMap);
    }
    return _map.get(batch);
  }
  
  
  @Override
  public void deleteBatch(Object batch) throws LoopException {
    _map.remove(batch);
  }
  
  
  @Override
  public void addToGroup(Object batch, AggregationKey key, MapTuple tuple) throws LoopException {
    getMap(batch).put(key, tuple);
  }

  @Override
  public boolean hasGroup(Object batch, AggregationKey key) throws LoopException {
    return getMap(batch).containsKey(key);
  }

  @Override
  public Iterator<MapTuple> getGroupIterator(Object batch, AggregationKey key) throws LoopException {
    return getMap(batch).get(key).iterator();
  }

  @Override
  public void deleteGroup(Object batch, AggregationKey key) throws LoopException {
    getMap(batch).removeAll(key);
  }

  @Override
  public Iterator<AggregationKey> keyIterator(Object batch) throws LoopException {
    return Sets.newHashSet(getMap(batch).keySet()).iterator();
  }

  @Override
  public void flush(Object batch) throws LoopException {
  }

  public Collection<Object> getBatches() {
    return this._map.keySet();
  }

  
}

