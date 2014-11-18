package com.zillabyte.motherbrain.flow.operations.multilang.builder;

import java.io.Serializable;

import net.sf.json.JSONObject;

import com.zillabyte.motherbrain.container.ContainerException;
import com.zillabyte.motherbrain.flow.Flow;
import com.zillabyte.motherbrain.flow.FlowCompilationException;



/**
 * Fetches flows.  
 * @author jake
 *
 */
public interface FlowFetcher extends Serializable {
  
  public Flow buildFlow(String flowId, JSONObject config) throws FlowCompilationException, ContainerException;
  public Flow buildFlow(String flowId) throws FlowCompilationException, ContainerException;
  
}
