package grandmotherbrain.flow.collectors.coordinated.support;

import grandmotherbrain.flow.collectors.coordinated.CoordinatedOutputCollector;

import java.io.Serializable;


/***
 * The point of this interface is to allow us to someday, implement a fancy tuple replay functionality.
 * i.e. automatically resend failed tuples.  The following objects are created in this factory because
 * they will all be tied to each other to make an efficient replay strategy.
 * @author jake
 *
 */
public interface CoordinatedOutputCollectorSupportFactory extends Serializable  {

  public TupleIdSet createTupleIdSet(CoordinatedOutputCollector op);

  public TupleIdGenerator createTupleIdGenerator(CoordinatedOutputCollector op);
  
  public FailedTupleHandler createFailedTupleHandler(CoordinatedOutputCollector op);

  public TupleIdMapper createTupleIdMapper(CoordinatedOutputCollector _collector);
  
}
