package grandmotherbrain.flow.config;

import java.util.Map;

import net.sf.json.JSONObject;

/*************************
 * Operation Config
 *************************/
public class OperationConfig extends UserConfig {

  private static final long serialVersionUID = 714737244481242463L;

  public OperationConfig(OperationConfig confg) {
    super.setAll(confg);
  }
  
  public OperationConfig() {
  }

  public static OperationConfig createEmpty() {
    return new OperationConfig();
  }

  public static OperationConfig createFromJSON(JSONObject json) {
    return OperationConfig.createEmpty().setAll(json);
  }

  

  public OperationConfig mergeWith(Map<String, Object> conf) {
    OperationConfig c = new OperationConfig();
    c._settings.putAll(this._settings);
    c._settings.putAll(conf);
    return c;
  }
  
  
}