package com.zillabyte.motherbrain.flow.operations.decorators;

import java.util.Collection;

import com.zillabyte.motherbrain.flow.Fields;
import com.zillabyte.motherbrain.flow.MapTuple;
import com.zillabyte.motherbrain.flow.operations.Operation;

import net.sf.json.JSONObject;

public class RemoveFields implements EmitDecorator {

  private static final long serialVersionUID = 2012719348428908341L;
  private Fields _fields;
  private Operation _operation;
  private String _originStream;

  public RemoveFields(Collection<String> fields) {
    this(new Fields(fields));
  }
  
  public RemoveFields(Fields fields) {
    _fields = fields;
  }

  public RemoveFields(JSONObject node) {
    this(node.getJSONArray("remove"));
  }

  @Override
  public MapTuple execute(MapTuple t) {
    for(String field : _fields) {
      t.remove(field);
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
