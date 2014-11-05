package grandmotherbrain.flow.collectors.coordinated;

public class BatchCompleteAckTuple extends BaseCoordTuple {

  /**
   * 
   */
  private static final long serialVersionUID = -4219286304063702341L;

  public BatchCompleteAckTuple(Object batch, Integer fromTask) {
    super(batch, fromTask);
  }

  @Override
  protected String className() {
    return "BatchAck";
  }
  
  
}
