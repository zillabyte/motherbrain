package grandmotherbrain.flow.rpc.queues;

import grandmotherbrain.flow.rpc.RPCSink;
import grandmotherbrain.flow.rpc.RPCSource;

import java.io.Serializable;

import org.eclipse.jdt.annotation.NonNullByDefault;

@NonNullByDefault
public interface QueueFactory extends Serializable {

  public InputQueue getInputQueue(RPCSource source);
  public OutputQueue getOutputQueue(RPCSink sink);
  
}
