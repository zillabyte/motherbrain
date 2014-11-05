package grandmotherbrain.flow.operations.builtin;

import grandmotherbrain.flow.Fields;
import grandmotherbrain.flow.MapTuple;
import grandmotherbrain.flow.aggregation.Aggregator;
import grandmotherbrain.flow.collectors.OutputCollector;
import grandmotherbrain.flow.operations.GroupBy;
import grandmotherbrain.top.MotherbrainException;
import net.sf.json.JSONObject;

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
