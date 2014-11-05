package grandmotherbrain.flow.operations.multilang;

import org.apache.log4j.Logger;

public interface MultiLangLogHandler {
  
  public void onStdErr(String s, Logger fallbackLogger);
  
  public void onStdOut(String s, Logger fallbackLogger);
  
  public void onSystemError(String s, Logger fallbackLogger);
  
  public void onSystemInfo(String s, Logger fallbackLogger);
  
}
