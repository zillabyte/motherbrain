package com.zillabyte.motherbrain.flow.operations;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.UUID;

import org.apache.log4j.FileAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.Priority;

import com.zillabyte.motherbrain.universe.Universe;
import com.zillabyte.motherbrain.utils.Utils;

public abstract class OperationLogger implements Serializable {

  private static final long serialVersionUID = -4995010296782665869L;      
  private static final SimpleDateFormat rfc3339 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
  private static final Logger _internalLog = Utils.getLogger(OperationLogger.class);
  static {
    rfc3339.setTimeZone(TimeZone.getTimeZone("GMT"));
  }

  protected String _procId;
  
  
  protected OperationLogger(String procId) {
    _procId = procId;
  }
  
  public String procId() {
    return _procId;
  }
  
  protected static final String toRFC3339(Date d) {
    return rfc3339.format(d).replaceAll("(\\d\\d)(\\d\\d)$", "$1:$2");
  }

  public void writeLog(String message, LogPriority priority) {
    try {
      writeLogInternal(message, priority);
    } catch (OperationLoggerException | InterruptedException e) {
      _internalLog.error("internal logging error: " + e);
    }
  }
  
  public void logError(Exception e) {
    String message = e.getMessage();
    message = message.replace("\\n", "\n");
    if(message != null) {
      for(String s : message.split("\n")) { // Apparently we need that many slashes to escape \n
        writeLog(s, LogPriority.ERROR);
      }
    }
    for(StackTraceElement s : e.getStackTrace()) {
      writeLog(s.toString(), LogPriority.ERROR);
    }
    if(e.getCause() != null) {
      writeLog("Caused by:", LogPriority.ERROR);
      logError((Exception) e.getCause());
    }
  }

  protected abstract void writeLogInternal(String message, LogPriority priority) throws InterruptedException, OperationLoggerException;

  public abstract String absoluteFilename();

  public void error(String message) {
    writeLog(message, LogPriority.ERROR);
  }
  
  public void info(String message) {
    writeLog(message, LogPriority.RUN);
  }

  public void startUp(String message) {
    writeLog(message, LogPriority.STARTUP);
  }
  
  public void hidden(String message) {
    writeLog(message, LogPriority.HIDDEN);
  }
  
  public enum LogPriority {
    STARTUP,
    ERROR,
    HIDDEN,
    SYSTEM,
    IPC,
    RUN,
    KILL
  }

  public static abstract class Base extends OperationLogger {
    /**
     * Serialization ID
     */
    private static final long serialVersionUID = 9203738033367330437L;
    protected String _flowId;
    protected static Logger log = Logger.getLogger(OperationLogger.class);

    public Base(String flowId, String opName) {
      super(opName);
      _flowId = flowId;
      log.info("initializing an operation logger for: "+_procId);
    }
    

    @SuppressWarnings("deprecation")
    @Override
    public void writeLog(String message, LogPriority priority) 
    {
      log.log(OperationLogger.Base.class.getName(), Priority.INFO, "[f" + _flowId + "][" + _procId.toString() + "][" + priority + "] " + message, null);
      try {
        writeLogInternal(message, priority);
      } catch (OperationLoggerException | InterruptedException e) {
        log.error("internal logging error:" + e);
      }
    }
  }

  public static class noOp extends OperationLogger {

    /***
     * 
     * @param defaultProcId
     */
    public noOp() {
      super("");
    }

    /**
     * Serialization ID
     */
    private static final long serialVersionUID = 9221643914582167040L;

    @Override
    protected void writeLogInternal(String message, LogPriority priority) {
      // System.err.println("noop:" + message);
    }

    @Override
    public String absoluteFilename() {
      return null;
    }

  }


  public static class Local extends Base {

    private static final long serialVersionUID = -3651914779884984185L;
    private transient org.apache.log4j.Logger _log = null; // do NOT make static

    private static final int MAX_LENGTH = 80 * 5; // Lines in display

    public Local(String flowId, String opName) {
      super(flowId, opName);
    }
    
    public String filename() {   
      return "flow_"+_flowId+".log";
    }

    public File root() {
      return Universe.instance().fileFactory().getFlowLoggingRoot(_flowId);
    }

    public File logFile() {
      return new File(root(), filename());
    }

    @Override
    public String absoluteFilename() {
      return logFile().getAbsolutePath();
    }

    @Override
    protected synchronized void writeLogInternal(final String msg, LogPriority priority) throws OperationLoggerException {
      String message = msg;
      if (msg == null) {
        // Do nothing... 
        return;
      }
      if (message.length() > MAX_LENGTH && !priority.equals(LogPriority.ERROR)) { // don't truncate errors
        message = message.substring(0, MAX_LENGTH) + "[...truncated]";
      }
      message = message.replace("\n", "\\n").replace("\r", "\\r");
      try {
        if (_log == null) {
          // NOTE: this is the logger that the USER sees.  Don't change unless you know what you're doing. 
          _log = Logger.getLogger(UUID.randomUUID().toString());
          _log.setAdditivity(false);
          _log.removeAllAppenders();
          _log.setLevel(Level.ALL);
          logFile().getParentFile().mkdirs();
          String filename = absoluteFilename();
          Layout layout = new PatternLayout("%m%n");
          FileAppender appender = new FileAppender(layout, filename, true);
          _log.addAppender(appender);
        }
        _log.info(
            toRFC3339(new Date()) + " flow_" + _flowId + "[" +
            _procId + "] - [" + priority + "] " +
            message);
      } catch(IOException e) {
        throw new OperationLoggerException(e);
      }
    }
  }
}
