package com.zillabyte.motherbrain.flow.collectors.coordinated;

public class PingTuple extends BaseCoordTuple {

  /**
   * 
   */
  private static final long serialVersionUID = 4252361085862436567L;

  private String _message;
  private CoordTupleOptions _options;
  
  public PingTuple(Object batch, Integer fromTask, String message) {
    this(batch, fromTask, message, CoordTupleOptions.build());
  }
  
  public PingTuple(Object batch, Integer fromTask, String message, CoordTupleOptions options) {
    super(batch, fromTask);
    _message = message;
    _options = options;
  }
  
  public String messsage() {
    return _message;
  }
  
  public CoordTupleOptions options() {
    return _options;
  }
  
  @Override
  protected String className() {
    return "Ping";
  }
  
  @Override
  protected String extraToString() {
    return " message=" + messsage() + " options =" + options(); 
  }
  
}
