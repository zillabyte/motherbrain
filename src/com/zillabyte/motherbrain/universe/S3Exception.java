package com.zillabyte.motherbrain.universe;

public class S3Exception extends Exception {
  /**
   * 
   */
  private static final long serialVersionUID = -5082670030279130135L;
  
  public S3Exception(Exception e) {
    super(e);
  }
  public S3Exception() {
    super();
  }
  public S3Exception(String string) {
    super(string);
  }
}
