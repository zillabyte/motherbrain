package com.zillabyte.motherbrain.flow.collectors.coordinated.support.naive;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;

import com.google.common.collect.Maps;
import com.zillabyte.motherbrain.flow.collectors.coordinated.support.TupleIdSet;

public class UncompressedTupleIdSet implements TupleIdSet {

  
  /**
   * 
   */
  private static final long serialVersionUID = 5819334273086106352L;
  
  private Map<Object, Long> _map = Maps.newHashMap();
  
  
  @Override
  public void add(Object t) {
    _map.put(t, System.currentTimeMillis());
  }

  
  @Override
  public void addAll(TupleIdSet set) {
    _map.putAll( ((UncompressedTupleIdSet)set)._map );
  }


  @Override
  public int size() {
    return _map.size();
  }

  
  @Override
  public UncompressedTupleIdSet getTupleIdsOlderThan(Long cutoffTime) {
    UncompressedTupleIdSet ret = new UncompressedTupleIdSet();
    for(Entry<Object, Long> e : _map.entrySet()) {
      if (e.getValue() < cutoffTime) {
        ret._map.put(e.getKey(), e.getValue());
      }
    }
    return ret;
  }

  
  @Override
  public Iterator<Object> iterator() {
    return _map.keySet().iterator();
  }

  
  @Override
  public void removeAll(TupleIdSet set) {
    
    if (set == this) {
      _map.clear();
      return;
    }
    
    UncompressedTupleIdSet iset = ((UncompressedTupleIdSet)set);
    for(Entry<Object, Long> e : iset._map.entrySet()) {
      _map.remove(e.getKey());
    }
  }


  @Override
  public void cleanup() {
    _map.clear();
  }
  
  @Override
  public String toString() {
    return new TreeSet(_map.keySet()).toString();
  }


  @Override
  public Long getOldest() {
    if (_map.size() == 0) return null;
    Long oldest = Long.MAX_VALUE;
    for(Long v : _map.values()) {
      oldest = Math.min(v, oldest);
    }
    return oldest;
  }
  
  
  @Override 
  public TupleIdSet clone() {
    UncompressedTupleIdSet n = new UncompressedTupleIdSet();
    n._map = Maps.newHashMap(_map);
    return n;
  }


  @Override
  public void setTimestamp(TupleIdSet set, Long time) {
    UncompressedTupleIdSet uset = (UncompressedTupleIdSet) set;
    for(Object k : uset._map.keySet()) {
      if (_map.containsKey(k)) {
        _map.put(k, time);
      }
    }
  }


  @Override
  public void remove(Object t) {
    _map.remove(t);
  }


  @Override
  public void clear() {
    _map.clear();
  }

}
