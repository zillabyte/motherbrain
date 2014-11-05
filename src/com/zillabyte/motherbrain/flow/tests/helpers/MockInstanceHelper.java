package com.zillabyte.motherbrain.flow.tests.helpers;

import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.util.List;

import org.apache.log4j.Logger;

import com.google.common.collect.ArrayListMultimap;
import com.zillabyte.motherbrain.flow.operations.Operation;


/***
 * A helper class to keep track of operation instances for testing
 * @author jake
 *
 */
public class MockInstanceHelper implements Serializable {
  
  
  /**
   * 
   */
  private static final long serialVersionUID = 2243095886448309808L;
  private static ArrayListMultimap<String, Operation> _instances = ArrayListMultimap.create();
  private static Logger _log = Logger.getLogger(MockInstanceHelper.class);
  
  public synchronized static void reset() {
    _log.info("resetting (" + ManagementFactory.getRuntimeMXBean().getName() + "): ");
    _instances = ArrayListMultimap.create();
  }
  
  
  /***
   * 
   * @param op
   */
  public synchronized static void registerInstance(Operation op) {
    _instances.put(op.namespaceName(), op);
  }
  
  
  public synchronized static List<Operation> getInstances(Operation operation) {
    return _instances.get(operation.namespaceName());
  }
  
  
  /***
   * 
   * @param operation
   */
  @SuppressWarnings("unchecked")
  public synchronized static <T extends Operation> T oneInstance(T operation) {
    List<Operation> c = getInstances(operation);
    if (c.size() > 1) throw new RuntimeException("> 1 instances!");
    if (c.size() == 0) throw new RuntimeException("no instances!");
    return (T) c.iterator().next();
  }
}
