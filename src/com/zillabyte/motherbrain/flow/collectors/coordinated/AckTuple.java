package com.zillabyte.motherbrain.flow.collectors.coordinated;

import com.zillabyte.motherbrain.flow.collectors.coordinated.support.TupleIdSet;

public class AckTuple extends BaseCoordTuple {

  /**
   * 
   */
  private static final long serialVersionUID = 7447603021552297437L;
  
  private TupleIdSet _tupleIdSet;

  public AckTuple(Object batch, Integer fromTask, TupleIdSet tupleIdSet) {
    super(batch, fromTask);
    _tupleIdSet = tupleIdSet.clone(); 
  }
  
  public TupleIdSet tupleIdSet() {
    return _tupleIdSet;
  }
  
  @Override
  protected String className() {
    return "Ack";
  }
  
  @Override 
  protected String extraToString() {
    return " tuples=" + _tupleIdSet.toString();
  }
  
}
