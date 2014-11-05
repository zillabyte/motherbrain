package com.zillabyte.motherbrain.container.local;

import java.io.File;

import com.zillabyte.motherbrain.container.ContainerFactory;
import com.zillabyte.motherbrain.container.ContainerSerializer;
import com.zillabyte.motherbrain.container.ContainerWrapper;
import com.zillabyte.motherbrain.container.NoopRemoteContainerCleaner;
import com.zillabyte.motherbrain.container.RemoteContainerCleaner;
import com.zillabyte.motherbrain.flow.config.FlowConfig;

public class InplaceContainerFactory implements ContainerFactory {

  /**
   * 
   */
  private static final long serialVersionUID = -4360587190481512961L;
  private File _flowRoot;
  
  public InplaceContainerFactory(File flowRoot) {
    _flowRoot = flowRoot;
  }
  
  public InplaceContainerFactory(String flowRoot) {
    this(new File(flowRoot));
  }

  @Override
  public ContainerWrapper createContainerFor(FlowConfig fc) {
    return new ContainerWrapper(new InplaceContainer(_flowRoot, fc));
  }

  @Override
  public ContainerSerializer createSerializer() {
    return new InplaceSerializer();
  }

  @Override
  public RemoteContainerCleaner createRemoteCleaner() {
    return new NoopRemoteContainerCleaner();
  }

}
