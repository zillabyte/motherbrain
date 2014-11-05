package com.zillabyte.motherbrain.flow.operations.multilang;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.Maps;
import com.zillabyte.motherbrain.coordination.CoordinationException;
import com.zillabyte.motherbrain.flow.operations.Operation;
import com.zillabyte.motherbrain.universe.Universe;
import com.zillabyte.motherbrain.utils.Utils;

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