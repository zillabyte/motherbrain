package com.zillabyte.motherbrain.flow.operations.decorators;

import java.util.Collection;
import java.util.Set;

import net.sf.json.JSONObject;

import com.google.monitoring.runtime.instrumentation.common.com.google.common.collect.Sets;
import com.zillabyte.motherbrain.flow.Fields;
import com.zillabyte.motherbrain.flow.MapTuple;
import com.zillabyte.motherbrain.flow.operations.Operation;

public class RetainFields implements EmitDecorator {

  private static final long serialVersionUID = 8968580455032090047L;
  private Fields _retainFields;
  private Operation _operation;

  public RetainFields(Collection<String> fields) {
    this(new Fields(fields));
  }
  
  public RetainFields(Fields fields) {
    _retainFields = fields;
  }
  

  public RetainFields(JSONObject node) {
    this(node.getJSONArray("retain"));
  }

  @Override
  public MapTuple execute(MapTuple t) {
    Set<String> keys = t.values().keySet();
    for(String field : Sets.newHashSet(keys)) {
      if (_retainFields.contains(field) == false) {
        keys.remove(field);
      }
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
