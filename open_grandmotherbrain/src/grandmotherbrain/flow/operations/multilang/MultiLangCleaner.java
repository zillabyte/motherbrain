package grandmotherbrain.flow.operations.multilang;

import grandmotherbrain.coordination.CoordinationException;
import grandmotherbrain.flow.operations.Operation;
import grandmotherbrain.universe.Universe;
import grandmotherbrain.utils.Utils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.Maps;

public class MultiLangCleaner {
  
  static final Map<String, Object> emptyMap = Collections.emptyMap();

  /***
   * 
   * @param process
   * @param op
   * @throws InterruptedException 
   * @throws CoordinationException 
   */
  public static void registerOperation(MultiLangProcess process, Operation op) throws InterruptedException, CoordinationException {
    
    // Capture some information for later
    HashMap<String, Object> map = Maps.newHashMap();
    
    map.put("host", Utils.getHost());
    map.put("pid", process.getPid());
    
    // Store it
    Universe.instance().state().setState(op.instanceStateKey() + "/multilang", map);
  }
}