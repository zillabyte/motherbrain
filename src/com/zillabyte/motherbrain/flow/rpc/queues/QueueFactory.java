package com.zillabyte.motherbrain.flow.rpc.queues;

import java.io.Serializable;

import org.eclipse.jdt.annotation.NonNullByDefault;

import com.zillabyte.motherbrain.flow.rpc.RPCSink;
import com.zillabyte.motherbrain.flow.rpc.RPCSource;

@NonNullByDefault
public interface QueueFactory extends Serializable {

  public InputQueue getInputQueue(RPCSource source);
  public OutputQueue getOutputQueue(RPCSink sink);
  
}
