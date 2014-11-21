package com.zillabyte.motherbrain.top;

import java.util.TimeZone;

import org.apache.log4j.Logger;

import com.zillabyte.motherbrain.api.APIException;
import com.zillabyte.motherbrain.flow.App;
import com.zillabyte.motherbrain.flow.Component;
import com.zillabyte.motherbrain.flow.FlowInstance;
import com.zillabyte.motherbrain.universe.Universe;
import com.zillabyte.motherbrain.utils.Utils;


public class BasicTopService implements TopService {

  private static Logger log = Utils.getLogger(BasicTopService.class);
  
  
  public BasicTopService() {
  }
  
  
  private static Universe universe() {
    return Universe.instance();
  }
  
  
  @Override
  public FlowInstance registerApp(App app) {  
    // Init 
    log.info("received new flow: " + app);
    return universe().flowService().registerApp(app);
  }

  @Override
  public void init() {

    java.util.TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    
    universe().flowService().init();
    universe().bufferService().init();
  }


  @Override
  public void shutDown() {
    universe().state().shutdown();
    universe().flowService().shutDown();
    universe().bufferService().shutDown();
  }


  @Override
  public void registerComponent(Component comp) {
    // Init 
    log.info("received new component: " + comp);
    
    // Give the flow to storm
    try {
      universe().flowService().registerComponent(comp);
    } catch (APIException e) {
      throw new RuntimeException(e);
    }
    
  }

  
//
//  @Override
//  public void refreshFlowState(FlowInstance flowInstance) {
//    universe().flowService().refreshFlowState(flowInstance);
//    
//  }
  
  
  


  
}


