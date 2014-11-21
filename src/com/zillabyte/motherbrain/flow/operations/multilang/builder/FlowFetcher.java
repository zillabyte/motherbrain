package com.zillabyte.motherbrain.flow.operations.multilang.builder;

import java.io.Serializable;

import net.sf.json.JSONObject;

import com.zillabyte.motherbrain.flow.Flow;



/**
 * Fetches flows.  
 * @author jake
 *
 */
public interface FlowFetcher extends Serializable {
  
  public Flow buildFlow(String flowId, JSONObject config);
  public Flow buildFlow(String flowId);
  
}
