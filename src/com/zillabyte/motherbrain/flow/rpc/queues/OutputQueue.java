package com.zillabyte.motherbrain.flow.rpc.queues;

import java.io.Serializable;

import com.zillabyte.motherbrain.flow.operations.OperationException;
import com.zillabyte.motherbrain.flow.rpc.RPCResponse;

public interface OutputQueue extends Serializable {

  public void sendResponse(RPCResponse response) throws OperationException;
  
  public void init();
  public void shutdown();
  
  
}
