package grandmotherbrain.flow;

import grandmotherbrain.flow.operations.Operation;
import grandmotherbrain.flow.operations.OperationException;

public class FlowStateException extends OperationException {


  private static final long serialVersionUID = 1422795079256403067L;

  public FlowStateException(Operation op, String string) {
    super(op, string);
  }
  
}
