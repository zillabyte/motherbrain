package com.zillabyte.motherbrain.flow.components.builtin;

import com.zillabyte.motherbrain.flow.Component;
import com.zillabyte.motherbrain.flow.Flow;
import com.zillabyte.motherbrain.flow.config.FlowConfig;

import net.sf.json.JSONObject;

public class BuiltinComponents {

  public static boolean exists(String name) {
    switch(name) {
    case "fetch_url": 
      return true;
    }
    return false;
  }
  
  
  public static Component create(String name, FlowConfig flowConfig) {
    switch(name) {
    case "fetch_url": 
      return FetchUrlComponent.create(flowConfig);
    }
    return null;
  }


  public static Flow create(String flowId, JSONObject overrideConfig) {
    return create(flowId, (FlowConfig) FlowConfig.createEmpty().setAll(overrideConfig));
  }
  
}
