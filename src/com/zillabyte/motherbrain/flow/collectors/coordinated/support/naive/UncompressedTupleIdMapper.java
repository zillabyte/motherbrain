package com.zillabyte.motherbrain.flow.collectors.coordinated.support.naive;

import java.util.Iterator;
import java.util.List;

import org.javatuples.Triplet;

import com.google.common.collect.Lists;
import com.zillabyte.motherbrain.flow.collectors.coordinated.support.TupleIdMapper;
import com.zillabyte.motherbrain.flow.collectors.coordinated.support.TupleIdSet;

public class UncompressedTupleIdMapper implements TupleIdMapper {

  private List<Triplet<Integer, Object, TupleIdSet>> _buffer = Lists.newLinkedList();
  private Integer _cachedSize = null;
  
  
  @Override
  public Iterator<Triplet<Integer, Object, TupleIdSet>> iterator() {
    return Lists.newArrayList(_buffer).iterator();
  }

  @Override
  public int getTupleSize() {
    int size = 0;
    for(Triplet<Integer, Object, TupleIdSet> ee : _buffer) {
      size += ee.getValue2().size();
    }
    return size;
  }

  @Override
  public void removeTupleIdValues(TupleIdSet tupleIds) {
    Long start = System.currentTimeMillis();
    for(Triplet<Integer, Object, TupleIdSet> ee : _buffer) {
      ee.getValue2().removeAll(tupleIds);
    }
  }

  @Override
  public void removeTupleIdKey(Object key) {
    for(Triplet<Integer, Object, TupleIdSet> ee : Lists.newArrayList(_buffer)) {
      if (ee.getValue1().equals(key)) {
        _buffer.remove(ee);
      }
    }
  }

  @Override
  public void addEntry(Integer upstreamTask, Object originId, Object tupleId) {
    for(Triplet<Integer, Object, TupleIdSet> ee : _buffer) {
      if (ee.getValue0() == upstreamTask) {
        if (ee.getValue1() == null) {
          if (originId == null) {
            ee.getValue2().add(tupleId);
            return;
          }
        } else if (ee.getValue1().equals(originId)) {
          ee.getValue2().add(tupleId);
          return;
        }
      }
    }
    TupleIdSet tupleSet = new UncompressedTupleIdSet();
    _buffer.add(new Triplet<Integer, Object, TupleIdSet>(upstreamTask, originId, tupleSet ));
    tupleSet.add(tupleId);
  }

  @Override
  public void ensureEntry(Integer upstreamTask, Object originTuple) {
    for(Triplet<Integer, Object, TupleIdSet> ee : _buffer) {
      if (ee.getValue0() == upstreamTask) {
        if (ee.getValue1() == null) {
          if (originTuple == null) {
            return;
          }
        } else if (ee.getValue1().equals(originTuple)) {
          return;
        }
      }
    }
    TupleIdSet tupleSet = new UncompressedTupleIdSet();
    _buffer.add(new Triplet<Integer, Object, TupleIdSet>(upstreamTask, originTuple, tupleSet ));
  }

  @Override
  public void setTimestamp(TupleIdSet set, long time) {
    for(Triplet<Integer, Object, TupleIdSet> ee : _buffer) {
      ee.getValue2().setTimestamp(set, time);
    }
  }

  
  @Override
  public String toString() {
    return _buffer.toString();
  }
  
}
