package grandmotherbrain.flow.aggregation;

import grandmotherbrain.flow.MapTuple;
import grandmotherbrain.flow.collectors.OutputCollector;
import grandmotherbrain.top.MotherbrainException;

public interface Aggregator {

  public abstract void start(MapTuple newGroupFieldValues) throws MotherbrainException, InterruptedException;
  
  public abstract void aggregate(MapTuple t, OutputCollector c) throws MotherbrainException, InterruptedException;
  
  public abstract void complete(OutputCollector c) throws MotherbrainException, InterruptedException;

  
}
