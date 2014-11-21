package com.zillabyte.motherbrain.container.local;

import org.apache.log4j.Logger;

import com.zillabyte.motherbrain.container.Container;
import com.zillabyte.motherbrain.container.ContainerSerializer;
import com.zillabyte.motherbrain.container.ContainerWrapper;
import com.zillabyte.motherbrain.flow.Flow;
import com.zillabyte.motherbrain.utils.Utils;

public class InplaceSerializer implements ContainerSerializer {

  /**
   * 
   */
  private static final long serialVersionUID = 8091126351698339609L;
  
  private static Logger _log = Utils.getLogger(InplaceSerializer.class);

  @Override
  public void serializeFlow(Flow flow) {
    // Noop
    return;
  }

  @Override
  public void deserializeOperationInstance(Container container, String instName) {
    if (container instanceof ContainerWrapper) {
      container = ((ContainerWrapper)container).getDelegate();
    }
    assert(container instanceof InplaceContainer);
    InplaceContainer c = (InplaceContainer) container;
    c.setStarted(false);
  }

  
}
