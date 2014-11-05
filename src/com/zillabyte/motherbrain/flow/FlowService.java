package com.zillabyte.motherbrain.flow;

import org.eclipse.jdt.annotation.NonNull;

import com.zillabyte.motherbrain.api.APIException;


/***
 * The FlowService is an abstraction of Storm (Trident).  It really provides two useful functions: `registerApp` 
 * for starting new apps on the Storm cluster, and `registerComponent` to register a component for later splicing.
 * 
 * Why make this an abstraction? Because the day will come when we want to dump storm and possibly build our own 
 * implementation.  By drawing this abstraction, we are no longer locked into Storm going forward. 
 *
 */
public interface FlowService {

  /**
   * Register an app (standalone or RPC)
   * @param app
   * @throws InterruptedException
   * @throws FlowCompilationException
   * @throws Exception 
   */
  @NonNull FlowInstance registerApp(App app) throws Exception;
  
  
  /**
   * Register a component within the system for later splicing into apps
   * 
   * @param comp
   * @throws InterruptedException
   * @throws FlowCompilationException
   * @throws APIException
   */
  void registerComponent(Component comp) throws InterruptedException, FlowCompilationException, APIException;

  void init();

  void shutDown();

  void killFlow(FlowInstance flow) throws InterruptedException, FlowException;

//  void refreshFlowState(FlowInstance flowInstance);


}
