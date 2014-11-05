package com.zillabyte.motherbrain.flow.operations.decorators;

import java.util.Map;
import java.util.Map.Entry;

import net.sf.json.JSONObject;

import com.google.monitoring.runtime.instrumentation.common.com.google.common.collect.Maps;
import com.zillabyte.motherbrain.flow.FlowCompilationException;
import com.zillabyte.motherbrain.flow.MapTuple;
import com.zillabyte.motherbrain.flow.operations.Operation;
import com.zillabyte.motherbrain.top.MotherbrainException;

public class RenameFields implements EmitDecorator {

  private static final long serialVersionUID = 1366152771280018239L;
  
  private Map<String, String> _renameMap;

  private Operation _operation;

  
  public RenameFields(JSONObject node) throws FlowCompilationException {
    if (node.has("rename")) {
      _renameMap = node.getJSONObject("rename");
    } else if (node.has("config") && node.getJSONObject("config").has("rename")) {
      _renameMap = node.getJSONObject("config").getJSONObject("rename");
    }
  }
  
  public RenameFields(Map<String, String> node)  {
    _renameMap = Maps.newHashMap();
    _renameMap.putAll(node);
  }
  
  

  @Override
  public MapTuple execute(MapTuple t) throws MotherbrainException {
    for(Entry<String, String> e : _renameMap.entrySet()) {
      if (e.getValue() == null) throw new FlowCompilationException("cannot map to null value: " + e.toString());
      Object val = t.get(e.getKey());
      t.remove(e.getKey());
      t.put(e.getValue(), val);
    }
    return t;
  }
  

  @Override
  public Operation getOperation() {
    return _operation;
  }

  @Override
  public void setOperation(Operation o) {
    _operation = o;
  }

}
