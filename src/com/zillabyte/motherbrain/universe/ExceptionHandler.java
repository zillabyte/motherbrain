package com.zillabyte.motherbrain.universe;

import java.io.Serializable;

import org.apache.log4j.Logger;


public class ExceptionHandler implements Serializable {
  
  private static final long serialVersionUID = 5401111722011936964L;
  private static Logger _log = Logger.getLogger("unhandled_exceptions");

  /**
   * @throws InterruptedException  
   * @throws CoordinationException 
   */
  public void handle(Throwable e) throws InterruptedException {
    e.printStackTrace();
    _log.error(e.getMessage());
    for(StackTraceElement s : e.getStackTrace()) {
      _log.error(s.toString());
    }
    
  }
  
  
  
  public static class RemoteReporter extends ExceptionHandler {
    private static final long serialVersionUID = 4846247203646024084L; 

    // public static final @NonNull String STATE_STREAM = "/unhanded_errors";
    
    @Override
    public void handle(Throwable e) throws InterruptedException {
      super.handle(e);
      // TODO
    } 
  }
  
  
  public static class NuclearHandler extends ExceptionHandler {
    private static final long serialVersionUID = 217270704929868213L;

    @Override
    public void handle(Throwable e) throws InterruptedException {
      super.handle(e);
      e.printStackTrace();
      System.exit(1);
    } 
  }

}
