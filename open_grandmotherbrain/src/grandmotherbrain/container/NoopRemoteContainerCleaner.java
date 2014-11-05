package grandmotherbrain.container;

import java.util.Set;


public class NoopRemoteContainerCleaner implements RemoteContainerCleaner {

  /**
   * 
   */
  private static final long serialVersionUID = 666374833830313266L;

  @Override
  public void cleanRemoteContainers(Set<String> hosts, Set<String> aliveFlowIds) {
    // Do nothing...
  }

}
