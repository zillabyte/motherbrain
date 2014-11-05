package grandmotherbrain.container.local;

import grandmotherbrain.container.Container;
import grandmotherbrain.container.ContainerException;
import grandmotherbrain.container.ContainerSerializer;
import grandmotherbrain.container.ContainerWrapper;
import grandmotherbrain.flow.Flow;
import grandmotherbrain.utils.Utils;

import org.apache.log4j.Logger;

public class InplaceSerializer implements ContainerSerializer {

  /**
   * 
   */
  private static final long serialVersionUID = 8091126351698339609L;
  
  private static Logger _log = Utils.getLogger(InplaceSerializer.class);

  @Override
  public void serializeFlow(Flow flow) throws ContainerException {
    // Noop
    return;
  }

  @Override
  public void deserializeOperationInstance(Container container, String instName) throws ContainerException {
    if (container instanceof ContainerWrapper) {
      container = ((ContainerWrapper)container).getDelegate();
    }
    assert(container instanceof InplaceContainer);
    InplaceContainer c = (InplaceContainer) container;
    c.setStarted(false);
  }

  
}
