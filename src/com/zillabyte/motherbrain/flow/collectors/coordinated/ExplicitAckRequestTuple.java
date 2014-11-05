package com.zillabyte.motherbrain.flow.collectors.coordinated;


public class ExplicitAckRequestTuple extends BaseCoordTuple {

  /**
   * 
   */
  private static final long serialVersionUID = 2356876586349193544L;

  public ExplicitAckRequestTuple(Object batch, Integer fromTask) {
    super(batch, fromTask);
  }

  @Override
  protected String className() {
    return "ExplicitAckRequest";
  }

}
