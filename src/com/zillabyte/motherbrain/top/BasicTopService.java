package com.zillabyte.motherbrain.top;

import java.sql.SQLException;
import java.util.TimeZone;

import org.apache.log4j.Logger;

import com.google.common.base.Throwables;
import com.zillabyte.motherbrain.api.APIException;
import com.zillabyte.motherbrain.coordination.CoordinationException;
import com.zillabyte.motherbrain.flow.App;
import com.zillabyte.motherbrain.flow.Component;
import com.zillabyte.motherbrain.flow.FlowCompilationException;
import com.zillabyte.motherbrain.flow.FlowInstance;
import com.zillabyte.motherbrain.universe.Universe;
import com.zillabyte.motherbrain.utils.JarCompilationException;
import com.zillabyte.motherbrain.utils.Utils;


public class BasicTopService implements TopService {

  private static Logger log = Utils.getLogger(BasicTopService.class);
  
  
  public BasicTopService() {
  }
  
  
  private static Universe universe() {
    return Universe.instance();
  }
  
  
  @Override
  public FlowInstance registerApp(App app) throws Exception {  
    // Init 
    log.info("received new flow: " + app);
    return universe().flowService().registerApp(app);
  }

  @Override
  public void init() throws InterruptedException, JarCompilationException, SQLException {

    java.util.TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    
    universe().flowService().init();
    universe().bufferService().init();
  }


  @Override
  public void shutDown() {
    try {
      universe().state().shutdown();
      universe().flowService().shutDown();
      universe().bufferService().shutDown();
    } catch(CoordinationException e) {
      Throwables.propagate(e);
    }
  }


  @Override
  public void registerComponent(Component comp) throws InterruptedException, FlowCompilationException, APIException {
    // Init 
    log.info("received new component: " + comp);
    
    // Give the flow to storm
    universe().flowService().registerComponent(comp);
    
  }

  
//
//  @Override
//  public void refreshFlowState(FlowInstance flowInstance) {
//    universe().flowService().refreshFlowState(flowInstance);
//    
//  }
  
  
  


  
}


