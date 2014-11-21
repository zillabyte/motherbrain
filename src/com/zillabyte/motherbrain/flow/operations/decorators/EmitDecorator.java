package com.zillabyte.motherbrain.flow.operations.decorators;

import java.io.Serializable;

import com.zillabyte.motherbrain.flow.MapTuple;
import com.zillabyte.motherbrain.flow.operations.LoopException;
import com.zillabyte.motherbrain.flow.operations.Operation;

public interface EmitDecorator extends Serializable {

  public MapTuple execute(MapTuple t) throws LoopException;

  public Operation getOperation();
  public void setOperation(Operation o);

  
}
