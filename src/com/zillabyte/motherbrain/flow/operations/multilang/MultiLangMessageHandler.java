package com.zillabyte.motherbrain.flow.operations.multilang;

import com.zillabyte.motherbrain.flow.operations.LoopException;

public interface MultiLangMessageHandler {

  public void handleMessage(String line) throws LoopException;
  
}
