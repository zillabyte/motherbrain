package com.zillabyte.motherbrain.flow.operations.multilang;

import com.zillabyte.motherbrain.flow.operations.OperationException;

public interface MultiLangMessageHandler {

  public void handleMessage(String line) throws InterruptedException, OperationException;
  
}
