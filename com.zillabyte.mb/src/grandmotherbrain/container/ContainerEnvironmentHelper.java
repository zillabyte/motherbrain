package grandmotherbrain.container;

import grandmotherbrain.flow.config.FlowConfig;
import grandmotherbrain.universe.Config;

import java.util.Map;

import com.google.common.collect.Maps;

public class ContainerEnvironmentHelper {

  public static Map<String, String> getCLIEnvironment(FlowConfig fc) {
    Map<String, String> map = Maps.newHashMap();
    map.put("ZILLABYTE_API_KEY", "user_" + fc.getUserId() + "@zillabyte.com," + fc.getAuthToken());
    map.put("ZILLABYTE_API_HOST", Config.getOrDefault("api.host", "localhost") );
    map.put("ZILLABYTE_API_PORT", Integer.toString(Config.getOrDefault("api.port", 5000)) );
    return map;
  }
  
}
