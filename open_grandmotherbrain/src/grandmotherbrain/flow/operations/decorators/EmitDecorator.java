package grandmotherbrain.flow.operations.decorators;

import grandmotherbrain.flow.MapTuple;
import grandmotherbrain.flow.operations.Operation;
import grandmotherbrain.top.MotherbrainException;

import java.io.Serializable;

public interface EmitDecorator extends Serializable {

  public MapTuple execute(MapTuple t) throws MotherbrainException;

  public Operation getOperation();
  public void setOperation(Operation o);

  
}
