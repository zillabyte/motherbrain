package grandmotherbrain.flow.collectors.coordinated;

public class BatchCompleteTuple extends BaseCoordTuple {

  /**
   * 
   */
  private static final long serialVersionUID = 4252361085862436567L;

  public BatchCompleteTuple(Object batch, Integer fromTask) {
    super(batch, fromTask);
  }
  
  @Override
  protected String className() {
    return "BatchComplete";
  }
  
}
