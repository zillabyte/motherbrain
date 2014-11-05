package com.zillabyte.motherbrain.flow.collectors.coordinated;

import com.zillabyte.motherbrain.flow.MapTuple;

/****
 * 
 * @author jake
 *
 */
public final class BatchedTuple extends MapTuple {
  
  private static final long serialVersionUID = 6927109005444544118L;
  private Object _batch;
  private Integer _subBatch;
  private Object _id;
  
  public BatchedTuple(MapTuple t, Object tupleId, Object batchId) {
    super(t);
    _batch = batchId;
    _id = tupleId;
  }
  
  public Object batchId() {
    return _batch;
  }
  
  public Integer subBatchId() {
    return _subBatch;
  }
  
//  @Override
//  public String toString() {
//    return this.toVerboseString();
//  }
  
  public String toVerboseString() {
    return "<#id=" + _id + " batch=" + _batch + " subBatch=" + _subBatch + " t=" + super.toString() + "#>";
  }

  public Object getId() {
    return _id;
  }
  
  public void setTupleId(Object o){
    _id = o;
  }
}