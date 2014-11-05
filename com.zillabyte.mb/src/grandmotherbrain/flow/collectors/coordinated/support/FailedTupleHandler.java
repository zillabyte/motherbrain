package grandmotherbrain.flow.collectors.coordinated.support;

import grandmotherbrain.flow.MapTuple;
import grandmotherbrain.flow.collectors.OutputCollector;

import java.io.Serializable;

public interface FailedTupleHandler extends Serializable {

  /***
   * Observes the mapping from tuple-id to actual tuple.  Used if the concrete impl wants to replay later on.
   * @param tupleId
   * @param tuple
   */
  public void observeTupleIdMapping(Object tupleId, MapTuple tuple);
  
  
  /***
   * Called when a tuple fails.  The impl may replay tuples and emit into the supplied collector. If
   * the impl does nothing, then the tuple will be lost. 
   * @param collector
   * @param batch
   * @param tupleId
   */
  public void handleFailedTupleIds(OutputCollector collector, Object batch, TupleIdSet tupleId);
  
  
}
