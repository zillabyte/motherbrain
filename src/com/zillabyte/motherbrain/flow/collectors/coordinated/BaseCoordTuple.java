package com.zillabyte.motherbrain.flow.collectors.coordinated;

import java.io.Serializable;

public abstract class BaseCoordTuple implements Serializable {

  /**
   * 
   */
  private static final long serialVersionUID = 7399926549805126324L;
  
  private Object _batchId;
  private Integer _fromTask;
  
  public BaseCoordTuple(Object batch, Integer fromTask) {
    _batchId = batch;
    _fromTask = fromTask;
  }
  
  public Object batchId() {
    return _batchId;
  }
  
  public Integer fromTaskId() {
    return _fromTask;
  }
  
  
  @Override
  public String toString() {
    return( "<" + className() + " fromTask=" + _fromTask + " batch=" + _batchId + " " + extraToString() + ">" );
  }

  
  protected abstract String className();
  
  
  protected String extraToString() {
    return "";
  }
  
}
