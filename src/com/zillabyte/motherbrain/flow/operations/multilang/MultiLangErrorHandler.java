package com.zillabyte.motherbrain.flow.operations.multilang;

import com.zillabyte.motherbrain.flow.operations.OperationException;

public interface MultiLangErrorHandler {

  
  public void handleError(Exception ex);
  
  public Exception getNextError();
 
  public void maybeThrowNextError() throws OperationException;
  
  
}
