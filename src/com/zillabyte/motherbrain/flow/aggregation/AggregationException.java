package com.zillabyte.motherbrain.flow.aggregation;

import com.zillabyte.motherbrain.top.MotherbrainException;

public class AggregationException extends MotherbrainException {

  /**
   * 
   */
  private static final long serialVersionUID = 6359238031407959440L;

  public AggregationException() {
    super();
  }
  
  public AggregationException(String string) {
    super(string);
  }

  public AggregationException(Exception ex) {
    super(ex);
  }
  
}
