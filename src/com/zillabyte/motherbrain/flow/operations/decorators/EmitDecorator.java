package com.zillabyte.motherbrain.flow.operations.decorators;

import java.io.Serializable;

import com.zillabyte.motherbrain.flow.MapTuple;
import com.zillabyte.motherbrain.flow.operations.Operation;
import com.zillabyte.motherbrain.top.MotherbrainException;

public interface EmitDecorator extends Serializable {

  public MapTuple execute(MapTuple t) throws MotherbrainException;

  public Operation getOperation();
  public void setOperation(Operation o);

  
}
