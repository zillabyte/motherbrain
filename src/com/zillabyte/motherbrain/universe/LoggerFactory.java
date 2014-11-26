package com.zillabyte.motherbrain.universe;

import java.io.Serializable;

import org.eclipse.jdt.annotation.NonNullByDefault;

import com.zillabyte.motherbrain.flow.operations.MockOperationLogger;
import com.zillabyte.motherbrain.flow.operations.OperationLogger;

@NonNullByDefault
public abstract class LoggerFactory implements Serializable {
  
  
  /**
   * 
   */
  private static final long serialVersionUID = -4460044353443580460L;



  /***
   * 
   * @param flowId
   * @param procId
   * @return
   */
  public abstract OperationLogger logger(String flowId, String procId, String authToken);
  
  
  public OperationLogger logger(String flowId, String procId) {
    return logger(flowId, procId, "_no_authtoken_");
  }

  
  public static final class Local extends LoggerFactory {
    /**
     * 
     */
    static final long serialVersionUID = 8066041520236515796L;

    @Override
    public OperationLogger logger(final String flowId, String procId, String authToken) {
      final OperationLogger.Local newLogger = new OperationLogger.Local(flowId, procId);
      return newLogger;
    }
  }
  
  
  
  public static final class Mock extends LoggerFactory {
    /**
     * 
     */
    private static final long serialVersionUID = 5717759393951929228L;

    @Override
    public OperationLogger logger(final String flowId, String procId, String authToken) {
      return new MockOperationLogger(flowId, procId);
    }
  }
}
