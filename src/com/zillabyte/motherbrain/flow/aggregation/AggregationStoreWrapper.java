package com.zillabyte.motherbrain.flow.aggregation;

import java.util.Iterator;

import com.zillabyte.motherbrain.flow.MapTuple;

public class AggregationStoreWrapper implements AggregationStore {
  
  /**
   * 
   */
  private static final long serialVersionUID = 8911593118581072234L;
  private AggregationStore _base;

  public AggregationStoreWrapper(AggregationStore base) {
    _base = base;
  }


  @Override
  public void addToGroup(Object batch, AggregationKey key, MapTuple tuple) throws AggregationException {
    _base.addToGroup(batch, key, tuple);
  }

  @Override
  public boolean hasGroup(Object batch, AggregationKey key) throws AggregationException {
    return _base.hasGroup(batch, key);
  }

  @Override
  public Iterator<MapTuple> getGroupIterator(Object batch, AggregationKey key) throws AggregationException {
    return _base.getGroupIterator(batch, key);
  }

  @Override
  public void deleteGroup(Object batch, AggregationKey key) throws AggregationException {
    _base.deleteGroup(batch, key);
  }

  @Override
  public void deleteBatch(Object batch) throws AggregationException {
    _base.deleteBatch(batch);
  }

  @Override
  public Iterator<AggregationKey> keyIterator(Object batch) throws AggregationException {
    return _base.keyIterator(batch);
  }

  @Override
  public void flush(Object batch) throws AggregationException {
    _base.flush(batch);
  }
  
  
  
}
