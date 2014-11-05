package grandmotherbrain.flow.rpc.queues;

import grandmotherbrain.flow.rpc.RPCRequest;

import java.io.Serializable;

import org.eclipse.jdt.annotation.NonNullByDefault;

@NonNullByDefault
public interface InputQueue extends Serializable {

  public RPCRequest getNextRequest() throws InterruptedException;
  public boolean nextRequestAvailable();
  
  public void init();
  public void shutdown();
  
}
