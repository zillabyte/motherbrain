package grandmotherbrain.flow.rpc.queues;

import grandmotherbrain.flow.operations.OperationException;
import grandmotherbrain.flow.rpc.RPCResponse;

import java.io.Serializable;

public interface OutputQueue extends Serializable {

  public void sendResponse(RPCResponse response) throws OperationException;
  
  public void init();
  public void shutdown();
  
  
}
