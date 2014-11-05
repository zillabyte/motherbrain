package com.zillabyte.motherbrain.container;

import java.io.Serializable;
import java.util.Set;

public interface RemoteContainerCleaner extends Serializable {

  public void cleanRemoteContainers(Set<String> hosts, Set<String> aliveFlowIds);
  
}
