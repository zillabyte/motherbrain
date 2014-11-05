package grandmotherbrain.flow;

import grandmotherbrain.flow.operations.Operation;
import grandmotherbrain.flow.operations.OperationException;

public class ActionTimeoutException extends OperationException {

  /**
   * 
   */
  private static final long serialVersionUID = -5311969934287427423L;

  public ActionTimeoutException(Operation o, String string) {
    super(o, string);
  }

}
