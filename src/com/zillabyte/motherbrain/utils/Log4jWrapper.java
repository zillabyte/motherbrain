package com.zillabyte.motherbrain.utils;

import java.io.Serializable;

import org.apache.log4j.Logger;
import org.apache.log4j.Priority;

import com.zillabyte.motherbrain.flow.operations.Operation;


@SuppressWarnings("all")
public class Log4jWrapper implements Serializable {

  private static final long serialVersionUID = 8188911994286289364L;
  private Operation _prefix;
  private Class<?> _clazz; 
  private transient Logger _base = null;;
  
  public Log4jWrapper(Class<?> clazz, Operation op) {
    _clazz = clazz;
    _prefix = op;
  }
  
  public String format(Object msg) {
    return "[" + _prefix.instanceName() + "] " + msg.toString();
  }
  
  
  private Logger base() {
    if (_base == null) {
      _base = Logger.getLogger(_clazz);
    }
    return _base;
  }
  
  public void info(Object msg) {
    log(Priority.INFO, msg);
  }
  
  private void log(Priority info, Object msg) {
    base().log(Log4jWrapper.class.getName(), info, format(msg), null);
  }

  public void error(Object msg) {
    log(Priority.ERROR, msg);
  }
  
  public void warn(Object msg) {
    log(Priority.WARN, msg);
  }
  
  public void debug(Object msg) {
    log(Priority.DEBUG, msg);
  }
  
  public static Log4jWrapper create(Class clazz, Operation o) {
    return new Log4jWrapper(clazz, o);
  }
  
}
