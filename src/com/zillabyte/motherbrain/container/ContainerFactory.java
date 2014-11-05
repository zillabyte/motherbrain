package com.zillabyte.motherbrain.container;

import java.io.Serializable;

import com.zillabyte.motherbrain.flow.config.FlowConfig;

/**
 * Responsible for generating new containers for sandboxing flows
 * @author sjarvie
 *
 */
public interface ContainerFactory extends Serializable {

  /**
   * Containers are flow specific
   * @param fc
   * @return
   */
  public ContainerWrapper createContainerFor(FlowConfig fc);

  /**
   * Create a serialzer for containers produced by this factory type
   * @return
   */
  public ContainerSerializer createSerializer();

  
  /**
   * Create a serialzer for containers produced by this factory type
   * @return
   */
  public RemoteContainerCleaner createRemoteCleaner();

  
}

