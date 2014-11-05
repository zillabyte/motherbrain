package grandmotherbrain.flow.operations.multilang;

import grandmotherbrain.flow.operations.OperationLogger;

import org.apache.log4j.Logger;

public class MultiLangProcessStartupLogObserver implements MultiLangLogHandler {

  private MultiLangProcess _proc;
  private OperationLogger _logger;

  
  public MultiLangProcessStartupLogObserver(MultiLangProcess proc, OperationLogger logger) {
    _proc = proc;
    _logger = logger;
    _proc.addLogListener(this);
  }
  
  public void detach() {
    _proc.removeLogListener(this);
  }
  
  @Override
  public void onStdErr(String s, Logger fallbackLogger) {
    _logger.writeLog(s, OperationLogger.LogPriority.ERROR);
  }

  @Override
  public void onStdOut(String s, Logger fallbackLogger) {
    _logger.writeLog(s, OperationLogger.LogPriority.RUN);
  }

  @Override
  public void onSystemError(String s, Logger fallbackLogger) {
    _logger.writeLog(s, OperationLogger.LogPriority.ERROR);
  }

  @Override
  public void onSystemInfo(String s, Logger fallbackLogger) {
    _logger.writeLog(s, OperationLogger.LogPriority.SYSTEM);
  }

}
