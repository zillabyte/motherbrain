package grandmotherbrain.flow.collectors.coordinated;

public class PongTuple extends BaseCoordTuple {

  /**
   * 
   */
  private static final long serialVersionUID = 4252361085862436567L;

  private String _reply;
  private CoordTupleOptions _options;
  
  public PongTuple(Object batch, Integer fromTask, String reply) {
    this(batch, fromTask, reply, CoordTupleOptions.build());
  }
  
  public PongTuple(Object batch, Integer fromTask, String reply, CoordTupleOptions options) {
    super(batch, fromTask);
    _reply = reply;
    _options = options;
  }
  public String reply() {
    return _reply;
  }
  
  public CoordTupleOptions options() {
    return _options;
  }
  
  @Override
  protected String className() {
    return "Pong";
  }
  
  @Override
  protected String extraToString() {
    return " reply=" + reply() + " options=" + options();
  }
  
  
}
