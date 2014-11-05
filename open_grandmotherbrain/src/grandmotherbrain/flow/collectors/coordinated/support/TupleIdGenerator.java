package grandmotherbrain.flow.collectors.coordinated.support;

import grandmotherbrain.flow.MapTuple;

import java.io.Serializable;

public interface TupleIdGenerator extends Serializable {

  public Object getTupleIdFor(MapTuple t);
  
}
