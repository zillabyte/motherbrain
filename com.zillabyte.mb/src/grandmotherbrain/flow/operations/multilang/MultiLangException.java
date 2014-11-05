package grandmotherbrain.flow.operations.multilang;

import grandmotherbrain.flow.operations.Operation;
import grandmotherbrain.flow.operations.OperationException;

public class MultiLangException extends OperationException {

  private static final long serialVersionUID = 4440690460543121143L;
  
  public MultiLangException(Operation o) {
    super(o);
  }
  
  public MultiLangException(Operation o, String msg) {
    super(o, msg);
  }

  public MultiLangException(Operation o, Throwable cause) {
    super(o, cause);
  }
  
  public MultiLangException(Operation o, String internalMesage, Throwable cause) {
    super(o, internalMesage, cause);
  }
  
}

