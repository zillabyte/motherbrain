package com.zillabyte.motherbrain.universe;

import java.io.Serializable;
import java.util.Map;

import org.apache.log4j.Logger;
import org.eclipse.jdt.annotation.NonNull;

import com.google.common.collect.Maps;
import com.zillabyte.motherbrain.coordination.CoordinationException;
import com.zillabyte.motherbrain.utils.DateHelper;
import com.zillabyte.motherbrain.utils.Utils;

public class ExceptionHandler implements Serializable {
  
  private static final long serialVersionUID = 5401111722011936964L;
  private static Logger _log = Logger.getLogger("unhandled_exceptions");

  /**
   * @throws InterruptedException  
   * @throws CoordinationException 
   */
  public void handle(Throwable e) throws InterruptedException, CoordinationException {
    e.printStackTrace();
    _log.error(e.getMessage());
    for(StackTraceElement s : e.getStackTrace()) {
      _log.error(s.toString());
    }
    
  }
  
  
  
  public static class RemoteReporter extends ExceptionHandler {
    private static final long serialVersionUID = 4846247203646024084L; 

    public static final @NonNull String STATE_STREAM = "/unhanded_errors";
    
    @Override
    public void handle(Throwable e) throws InterruptedException, CoordinationException {
      super.handle(e);
      Map<String, Object> map = Maps.newHashMap();
      map.put("error", e);
      map.put("time", DateHelper.formattedDate());
      map.put("machine", Utils.getHost());
      Universe.instance().state().sendMessage(STATE_STREAM, map);
    } 
  }
  
  
  public static class NuclearHandler extends ExceptionHandler {
    private static final long serialVersionUID = 217270704929868213L;

    @Override
    public void handle(Throwable e) throws InterruptedException, CoordinationException {
      super.handle(e);
      e.printStackTrace();
      System.exit(1);
    } 
  }

}
