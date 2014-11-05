package grandmotherbrain.flow.collectors.coordinated;

import grandmotherbrain.flow.collectors.coordinated.support.TupleIdSet;

public class QueuedTuple extends BaseCoordTuple {

  /**
   * 
   */
  private static final long serialVersionUID = 7447603021552297437L;
  
  private TupleIdSet _tupleIdSet;

  public QueuedTuple(Object batch, Integer fromTask, TupleIdSet tupleIdSet) {
    super(batch, fromTask);
    _tupleIdSet = tupleIdSet.clone(); 
  }
  
  public TupleIdSet tupleIdSet() {
    return _tupleIdSet;
  }
  
  @Override
  protected String className() {
    return "Queued";
  }
  
  @Override 
  protected String extraToString() {
    return " tuples=" + _tupleIdSet.toString();
  }
  
}
