package grandmotherbrain.flow.aggregation;

import grandmotherbrain.flow.operations.AggregationOperation;

import java.io.Serializable;

public interface AggregationStoreFactory extends Serializable {

  public AggregationStoreWrapper getStore(AggregationOperation op, String prefix);
  
}
