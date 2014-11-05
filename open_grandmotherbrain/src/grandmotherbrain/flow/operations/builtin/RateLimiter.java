package grandmotherbrain.flow.operations.builtin;

import grandmotherbrain.flow.MapTuple;
import grandmotherbrain.flow.collectors.OutputCollector;
import grandmotherbrain.flow.operations.Function;
import grandmotherbrain.flow.operations.OperationException;
import net.sf.json.JSONObject;

public class RateLimiter extends Function {


  private static final long serialVersionUID = -102744550587588987L;
  private Double _allowancesPerSecond= null;

  
  public RateLimiter(String name, Double allowancesPerSecond) {
    super(name);
    _allowancesPerSecond = allowancesPerSecond;
  }
  
  public RateLimiter(JSONObject node) {
    this(node.getString("name"), node.getJSONObject("config").getDouble("rate"));
  }

  
  @Override
  protected void process(MapTuple t, OutputCollector c) throws OperationException, InterruptedException {
    if (_allowancesPerSecond != null && _allowancesPerSecond == 0.0) {
      _sleeper.sleepFor((long) (1.0 / _allowancesPerSecond));
    }
    c.emit(t);
  }

}
