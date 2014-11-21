package com.zillabyte.motherbrain.universe;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

import org.apache.log4j.Logger;
import org.eclipse.jdt.annotation.NonNull;

import com.google.common.collect.Maps;
import com.zillabyte.motherbrain.api.APIException;
import com.zillabyte.motherbrain.coordination.CoordinationException;
import com.zillabyte.motherbrain.flow.operations.Operation;
import com.zillabyte.motherbrain.flow.operations.OperationException;
import com.zillabyte.motherbrain.flow.operations.OperationLogger;
import com.zillabyte.motherbrain.utils.DateHelper;
import com.zillabyte.motherbrain.utils.Utils;

public class ExceptionHandler implements Serializable {
  
  private static final long serialVersionUID = 5401111722011936964L;
  private static Logger _log = Logger.getLogger("unhandled_exceptions");
  private OperationLogger _logger;

  /**
   * @throws InterruptedException  
   * @throws CoordinationException 
   */
//  public void handle(Throwable e) throws InterruptedException, CoordinationException {
//    e.printStackTrace();
//    _log.error(e.getMessage());
//    for(StackTraceElement s : e.getStackTrace()) {
//      _log.error(s.toString());
//    }
//    
//  }
  public void setLogger(OperationLogger logger) {
    _logger = logger;
  }
  
  public <T> T throwError(Class<T> errorType, Object... obj) {
    Operation op = null;
    String message = null;
    Throwable throwable = null;
    
    if(obj.length == 3) {
      op = (Operation) obj[0];
      message = (String) obj[1];
      throwable = (Throwable) obj[2];
    } else if(obj.length == 2) {
      if(obj[0] instanceof Operation) {
        op = (Operation) obj[0];
        if(obj[1] instanceof String) {
          message = (String) obj[1];
          _logger.writeLog(message, OperationLogger.LogPriority.ERROR);
        } else if(obj[1] instanceof Throwable) {
          throwable = (Throwable) obj[1];
          _logger.writeLog(((Throwable) obj[1]).getMessage(), OperationLogger.LogPriority.ERROR);
        }
      } else {
        message = (String) obj[0];
        throwable = (Throwable) obj[1];
      }
    } else if(obj.length == 1) {
      if(obj[0] instanceof String) {
        message = (String) obj[0];
        _logger.writeLog(message, OperationLogger.LogPriority.ERROR);
      } else if(obj[0] instanceof Throwable) {
        throwable = (Throwable) obj[0];
        _logger.writeLog(((Throwable) obj[0]).getMessage(), OperationLogger.LogPriority.ERROR);
      }
    } else {
      throw new RuntimeException("Wrong length of inputs to throwError.");
    }

    switch(errorType.getSimpleName()) {
    case "RuntimeException":
      return (T) new RuntimeException(message, throwable);
    case "IOException":
      return (T) new IOException(message, throwable);
    case "InterruptedException":
      return (T) new InterruptedException(message);
    case "APIException":
      return (T) new APIException(message);
    case "OperationException":
      return (T) new OperationException(op, message, throwable);
    default:
      throw new RuntimeException("Unrecognized error type "+errorType.getSimpleName()+"!");
    }
  }
  
//  
//  
//  public static class RemoteReporter extends ExceptionHandler {
//    private static final long serialVersionUID = 4846247203646024084L; 
//
//    // public static final @NonNull String STATE_STREAM = "/unhanded_errors";
//    
//    @Override
//    public void handle(Throwable e) throws InterruptedException, CoordinationException {
//      super.handle(e);
//      // TODO
//    } 
//  }
//  
//  
//  public static class NuclearHandler extends ExceptionHandler {
//    private static final long serialVersionUID = 217270704929868213L;
//
//    @Override
//    public void handle(Throwable e) throws InterruptedException, CoordinationException {
//      super.handle(e);
//      e.printStackTrace();
//      System.exit(1);
//    } 
//  }

}
