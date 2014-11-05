package com.zillabyte.motherbrain.utils;

import com.zillabyte.motherbrain.flow.FlowCompilationException;


public class JarCompilationException extends FlowCompilationException {

  /**
   * 
   */
  private static final long serialVersionUID = 284596101633420635L;

  public JarCompilationException(Throwable e) {
    super(e);
  }
  
  public JarCompilationException() {
    super();
  }

  public JarCompilationException(String string) {
    super(string);
  }
}
