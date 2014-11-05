package grandmotherbrain.flow.operations.multilang;

import grandmotherbrain.flow.operations.OperationException;

public interface MultiLangMessageHandler {

  public void handleMessage(String line) throws InterruptedException, OperationException;
  
}
