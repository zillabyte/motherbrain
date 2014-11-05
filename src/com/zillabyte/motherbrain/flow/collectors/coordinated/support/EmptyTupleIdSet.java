package com.zillabyte.motherbrain.flow.collectors.coordinated.support;

import java.util.Iterator;

import com.google.common.collect.Iterators;

public final class EmptyTupleIdSet implements TupleIdSet {

  /**
   * 
   */
  private static final long serialVersionUID = 6597632044060629248L;

  @Override
  public void add(Object t) {
    throw new IllegalStateException("this is an immutable empty tupleset");
  }

  @Override
  public void addAll(TupleIdSet set) {
    throw new IllegalStateException("this is an immutable empty tupleset");
  }

  @Override
  public void removeAll(TupleIdSet set) {
    // Do nothing
  }

  @Override
  public int size() {
    return 0;
  }

  @Override
  public TupleIdSet getTupleIdsOlderThan(Long time) {
    return this;
  }

  @Override
  public Iterator<Object> iterator() {
    return Iterators.emptyIterator();
  }

  @Override
  public void cleanup() {
    
  }

  @Override
  public Long getOldest() {
    return null;
  }
  
  @Override 
  public TupleIdSet clone() {
    return this;
  }

  @Override
  public void setTimestamp(TupleIdSet set, Long time) {
  }

  @Override
  public void remove(Object t) {
  }

  @Override
  public void clear() {    
  }
  
}
