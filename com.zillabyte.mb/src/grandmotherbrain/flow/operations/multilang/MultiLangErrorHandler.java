package grandmotherbrain.flow.operations.multilang;

import grandmotherbrain.flow.operations.OperationException;

public interface MultiLangErrorHandler {

  
  public void handleError(Exception ex);
  
  public Exception getNextError();
 
  public void mabyeThrowNextError() throws OperationException;
  
  
}
