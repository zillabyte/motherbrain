package com.zillabyte.motherbrain.flow.operations.multilang.builder;

import java.util.Map;
import java.util.concurrent.TimeoutException;

import com.google.monitoring.runtime.instrumentation.common.com.google.common.collect.Maps;
import com.zillabyte.motherbrain.coordination.CoordinationException;
import com.zillabyte.motherbrain.flow.StateMachineException;
import com.zillabyte.motherbrain.flow.operations.Operation;


/***
 * This palceholder is used in the flow compilation process, mostly to keep track of EmitDecorators while the graph is being built. 
 */
public class PlaceHolderOperation<T> extends Operation {
  
  T _object;
  static int _count = 0;
  static Map<Object, PlaceHolderOperation> _map = Maps.newHashMap();
  
  public PlaceHolderOperation(String name, T value) {
    super(name);
    _object = value;
  }
  
  
  
  public static <T> PlaceHolderOperation<T> getOrCreate(T val) {
    if (_map.containsKey(val) == false) {
      _map.put(val, new PlaceHolderOperation(val));
    } 
    return _map.get(val);
  }
  
  public PlaceHolderOperation(T value) {
    this("placeholder_" + getIncCount(), value);
  }
  
  private synchronized static int getIncCount() {
    return _count++;
  }
  
  public T getObject() {
    return _object;
  }

  private static final long serialVersionUID = 5080217136626752777L;
  

  @Override
  public String type() {
    return "place_holder";
  }

  @Override
  public void transitionToState(String s, boolean transactional) throws StateMachineException, TimeoutException, CoordinationException {
    throw new IllegalStateException();
  }

  @Override
  public String getState() {
    throw new IllegalStateException();
  }

}
