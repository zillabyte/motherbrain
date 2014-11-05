package com.zillabyte.motherbrain.flow.rpc.queues;

import java.io.Serializable;

import org.eclipse.jdt.annotation.NonNullByDefault;

import com.zillabyte.motherbrain.flow.rpc.RPCRequest;

@NonNullByDefault
public interface InputQueue extends Serializable {

  public RPCRequest getNextRequest() throws InterruptedException;
  public boolean nextRequestAvailable();
  
  public void init();
  public void shutdown();
  
}
