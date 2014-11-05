package com.zillabyte.motherbrain.top;

import java.sql.SQLException;

import org.eclipse.jdt.annotation.NonNull;

import com.zillabyte.motherbrain.api.APIException;
import com.zillabyte.motherbrain.flow.App;
import com.zillabyte.motherbrain.flow.Component;
import com.zillabyte.motherbrain.flow.FlowCompilationException;
import com.zillabyte.motherbrain.flow.FlowInstance;
import com.zillabyte.motherbrain.utils.JarCompilationException;


/****
 * TopService
 * 
 * TopService is the top of the service hierarchy.  Functionally, it is responsible for receiving
 * flows and delegating them to sub-services. (i.e. push it down to the PostgresService, etc). 
 *
 */
public interface TopService {
  
  
  /***
   * The entry point for all apps. 
   * @param app
   * @throws InterruptedException 
   * @throws FlowCompilationException 
   * @throws Exception 
   */
  @NonNull FlowInstance registerApp(App app) throws InterruptedException, FlowCompilationException, Exception;

  
  /***
   * Called after the service is created. Also call sub-services 
   * @throws InterruptedException 
   * @throws JarCompilationException 
   * @throws SQLException 
   */
  void init() throws InterruptedException, JarCompilationException, SQLException;

  
  /***
   * Called to stop the service.  Also call sub-services
   */
  void shutDown();


  // Register a component for later splicing
  void registerComponent(Component comp) throws InterruptedException, FlowCompilationException, APIException;

  
//  /**
//   * Actually figure out the state from the service, 
//   * instead of relying on our tracking.
//   * @param flowInstance
//   */
//  void refreshFlowState(FlowInstance flowInstance);
// 
}
