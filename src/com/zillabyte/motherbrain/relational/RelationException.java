package com.zillabyte.motherbrain.relational;

import com.zillabyte.motherbrain.top.MotherbrainException;

public class RelationException extends MotherbrainException {

  /**
   * 
   */
  private static final long serialVersionUID = -6936701172235665943L;
  
  public RelationException() {}

  public RelationException(Throwable ex) {
    super(ex);
  }

  public RelationException(String s) {
    super(s);
  }

  public RelationException(String s, Throwable ex) {
    super(s, ex);
  }
}
