package grandmotherbrain.relational.naivepostgresimpl;

import grandmotherbrain.relational.StreamMarker;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.collect.Maps;

public class NaivePostgresStreamMarker implements StreamMarker {

  /**
   * 
   */
  private static final long serialVersionUID = 9097783050713722760L;
  private Map<String, Long> _map = Maps.newHashMap();
  
  public Long getLatest(String table) {
    return _map.get(table);
  }
  
  public void setLatest(String table, Long id) {
    _map.put(table, id);
  }
  
  public Set<Entry<String, Long>> entries() {
    return _map.entrySet();
  }
  
}
