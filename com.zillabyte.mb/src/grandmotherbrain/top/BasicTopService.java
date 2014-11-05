package grandmotherbrain.top;

import grandmotherbrain.api.APIException;
import grandmotherbrain.coordination.CoordinationException;
import grandmotherbrain.flow.App;
import grandmotherbrain.flow.Component;
import grandmotherbrain.flow.FlowCompilationException;
import grandmotherbrain.flow.FlowInstance;
import grandmotherbrain.universe.Universe;
import grandmotherbrain.utils.JarCompilationException;
import grandmotherbrain.utils.Utils;

import java.sql.SQLException;
import java.util.TimeZone;

import org.apache.log4j.Logger;

import com.google.common.base.Throwables;


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


