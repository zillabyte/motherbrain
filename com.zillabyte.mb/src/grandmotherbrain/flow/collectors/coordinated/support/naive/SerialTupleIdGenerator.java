package grandmotherbrain.flow.collectors.coordinated.support.naive;

import grandmotherbrain.flow.MapTuple;
import grandmotherbrain.flow.collectors.coordinated.CoordinatedOutputCollector;
import grandmotherbrain.flow.collectors.coordinated.support.TupleIdGenerator;

public class SerialTupleIdGenerator implements TupleIdGenerator {

  /**
   * 
   */
  private static final long serialVersionUID = 6342870024362338225L;
  
  private Long _current = 0L;
  private CoordinatedOutputCollector _col;
  
  public SerialTupleIdGenerator(CoordinatedOutputCollector c) {
    _col = c;
  }
  
  @Override
  public Object getTupleIdFor(MapTuple t) {
    return "t" + _col.getTaskId() + "_" + _current++;
  }

}
