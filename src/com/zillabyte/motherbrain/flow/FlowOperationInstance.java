package com.zillabyte.motherbrain.flow;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.javatuples.Pair;

import com.google.common.collect.Maps;
import com.google.monitoring.runtime.instrumentation.common.com.google.common.collect.Lists;

public class FlowOperationInstance {

  private String _id;
  private String _state = "UNKNOWN";
  private Long _lastUpdated = System.currentTimeMillis();
  private Map<String,Object> _info = Maps.newHashMap();
  private Map<String,String> _stats = Maps.newHashMap();
  private LinkedList<Pair<Exception, Long>> _recentErrors = Lists.newLinkedList();
  

  public FlowOperationInstance(String id) {
    _id = id;
  }

  public Map<String, Object> getInfo() {
    return Collections.unmodifiableMap(_info); 
  }
  
  public synchronized void updateInfo(Map<String, Object> info) {
    _info.putAll(info);
    _lastUpdated = System.currentTimeMillis();
  }
  

  public Map<String, String> getStats() {
    return Collections.unmodifiableMap(_stats);
  }

  public String getState() {
    return _state;
  }

  public Long lastUpdateTime() {
    return _lastUpdated;
  }

  public String getId() {
    return _id;
  }

  public Collection<Pair<Exception, Long>> getRecentErrorsWithDate() {
    // Make a copy so we don't end up with concurrent modification exceptions...this is costly...
    List<Pair<Exception, Long>> ret = Lists.newLinkedList();
    for(Pair<Exception, Long> p : _recentErrors) {
      ret.add(new Pair<Exception, Long>(p.getValue0(), p.getValue1()));
    }
    return ret;
  }
  
  public Collection<Exception> getRecentErrors() {
    List<Exception> ret = Lists.newLinkedList();
    for(Pair<Exception, Long> p : _recentErrors) {
      ret.add(p.getValue0());
    }
    return ret;
  }

  public synchronized void updateState(String state) {
    _state = state;
    _lastUpdated = System.currentTimeMillis();
  }

  public synchronized void addRecentError(Exception ex) {
    _recentErrors.add(new Pair<>(ex, System.currentTimeMillis()));
    _lastUpdated = System.currentTimeMillis();
    if (_recentErrors.size() > 5) {
      _recentErrors.removeFirst();
    }
  }

  public synchronized void updateStats(Map<String, String> stats) {
    _stats.putAll(stats);
    _lastUpdated = System.currentTimeMillis();
  }

  public Map<String, Object> getJSONDetails() {
    Map<String,Object> details = Maps.newHashMap();
    details.put("id", _id);
    details.put("state", _state);
    details.put("last_updated", _lastUpdated);
    details.put("info", _info);
    details.put("stats", _stats);
    details.put("recent_errors", getRecentErrors());
    return details;
  }

  public String getType() {
    return (String)this.getInfo().get("type");
  }

  public Object getOperationId() {
    return (String)this.getInfo().get("operation_id");
  }


}
