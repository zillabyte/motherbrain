package com.zillabyte.motherbrain.flow;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.google.monitoring.runtime.instrumentation.common.com.google.common.collect.Lists;

public class FlowOperationInstanceCollection implements Iterable<FlowOperationInstance> {

  
  private ConcurrentHashMap<String, FlowOperationInstance> _map;
  
  
  public FlowOperationInstanceCollection() {
    _map = new ConcurrentHashMap<>();
  }
  
  public FlowOperationInstanceCollection(FlowOperationInstanceCollection f) {
    _map = new ConcurrentHashMap<>(f._map);
  }
  
  
  public FlowOperationInstanceCollection clone() {
    return new FlowOperationInstanceCollection(this);
  }
  

  @Override
  public Iterator<FlowOperationInstance> iterator() {
    return _map.values().iterator();
  }

  public synchronized void clear() {
    _map.clear();
  }



  public boolean contains(String id) {
    return _map.containsKey(id);
  }


  public FlowOperationInstance getOrCreate(String id) {
    if (_map.containsKey(id) == false) {
      synchronized(this) {
        if (_map.containsKey(id) == false) {
          _map.put(id, new FlowOperationInstance(id));
        }
      }
    }
    return _map.get(id);
  }


  public FlowOperationInstance get(String instanceId) {
    return _map.get(instanceId);
  }
  
  
  public Collection<FlowOperationInstance> getByOperation(String opId) {
    List<FlowOperationInstance> ret = Lists.newLinkedList();
    for(FlowOperationInstance inst : this._map.values()) {
      if (opId.equals(inst.getOperationId())) {
        ret.add(inst);
      }
    }
    return ret;
  }


  public Set<String> idSet() {
    return _map.keySet();
  }


  public void remove(Object id) {
    _map.remove(id);
  }

  public List<Map<String,Object>> getJSONDetails() {
    List<Map<String,Object>> ret = Lists.newLinkedList();
    for(FlowOperationInstance inst : this._map.values()) {
      ret.add(inst.getJSONDetails());
    }
    return ret;
  }
  
  
}
