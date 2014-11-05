package grandmotherbrain.container.local;

import grandmotherbrain.container.ContainerFactory;
import grandmotherbrain.container.ContainerSerializer;
import grandmotherbrain.container.ContainerWrapper;
import grandmotherbrain.container.NoopRemoteContainerCleaner;
import grandmotherbrain.container.RemoteContainerCleaner;
import grandmotherbrain.flow.config.FlowConfig;

import java.io.File;

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
