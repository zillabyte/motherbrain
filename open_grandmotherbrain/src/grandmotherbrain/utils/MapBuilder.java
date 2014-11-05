package grandmotherbrain.utils;

import java.util.HashMap;
import java.util.Map;

import org.eclipse.jdt.annotation.NonNullByDefault;

@NonNullByDefault
public class MapBuilder<V> {

  private Map<String, V> _map = new HashMap<>();

  
  public MapBuilder<V> put(String key, V value) {
    _map .put(key, value);
    return this;
  }
  
  public Map<String, V> build() {
    return _map;
  }
  
  public static <V> MapBuilder<V> create() {
    return new MapBuilder<>();
  }
  
  
  public static Map<String, Object> create(String key, Object val) {
    return create().put(key, val).build();
  }
  
  
}
