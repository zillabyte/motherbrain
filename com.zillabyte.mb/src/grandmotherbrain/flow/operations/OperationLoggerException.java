package grandmotherbrain.flow.operations;

import grandmotherbrain.top.MotherbrainException;

public class OperationLoggerException extends MotherbrainException {

  /**
   * 
   */
  private static final long serialVersionUID = -14351183952883968L;
  
  public OperationLoggerException(String string) {
    super(string);
  }

  public OperationLoggerException(Throwable ex) {
    super(ex);
  }

  public OperationLoggerException(String s, Throwable ex) {
    super(s, ex);
  }
}
