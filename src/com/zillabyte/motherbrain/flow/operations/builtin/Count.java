package com.zillabyte.motherbrain.flow.operations.builtin;

import net.sf.json.JSONObject;

import com.zillabyte.motherbrain.flow.Fields;
import com.zillabyte.motherbrain.flow.MapTuple;
import com.zillabyte.motherbrain.flow.aggregation.Aggregator;
import com.zillabyte.motherbrain.flow.collectors.OutputCollector;
import com.zillabyte.motherbrain.flow.operations.GroupBy;
import com.zillabyte.motherbrain.top.MotherbrainException;

public class Count extends GroupBy implements Aggregator  {

  private static final long serialVersionUID = 2830639154969563830L;
  private long _count = 0L;
  private MapTuple _values;
  

  public Count(String name, Fields groupFields) {
    super(name, groupFields);
    _aggregator = this;
  }

  
  public Count(JSONObject node) {
    this(node.getString("name"), new Fields(node.getJSONArray("group_fields")));
  }


  @Override
  public void start(MapTuple newGroupFieldValues) throws MotherbrainException, InterruptedException {
    _count  = 0L;
    _values = newGroupFieldValues;
  }

  @Override
  public void aggregate(MapTuple t, OutputCollector c) throws MotherbrainException, InterruptedException {
    _count++;
  }

  @Override
  public void complete(OutputCollector c) throws MotherbrainException, InterruptedException {
    _values.put("count", _count);
    c.emit(_values);
  }

}
