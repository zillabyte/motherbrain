package grandmotherbrain.flow.collectors.coordinated;

import java.io.Serializable;

import net.sf.json.JSONObject;

public class CoordTupleOptions implements Serializable {
  /**
   * 
   */
  private static final long serialVersionUID = 368100573334064110L;
  
  JSONObject _options;
  
  public CoordTupleOptions(JSONObject options) {
    _options = options;
  }
  
  public static CoordTupleOptions build() {
    return new CoordTupleOptions(new JSONObject());
  }
  
  public CoordTupleOptions addOpt(String key, Object value) {
    _options.put(key, value);
    return this;
  }
  
  public Integer getInt(String key) {
    return _options.getInt(key);
  }
  
  public String getString(String key) {
    return _options.getString(key);
  }
  
  public Long getLong(String key) {
    return _options.getLong(key);
  }
  
  public Boolean getBoolean(String key) {
    return _options.getBoolean(key);
  }

  @Override
  public String toString() {
    return _options.toString();
  }
  
}
