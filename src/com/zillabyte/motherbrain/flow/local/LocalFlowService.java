package com.zillabyte.motherbrain.flow.local;

import java.util.Map;

import com.google.common.base.Throwables;
import com.google.monitoring.runtime.instrumentation.common.com.google.common.collect.Maps;
import com.google.monitoring.runtime.instrumentation.common.com.google.common.collect.Sets;
import com.zillabyte.motherbrain.flow.App;
import com.zillabyte.motherbrain.flow.Component;
import com.zillabyte.motherbrain.flow.FlowInstance;
import com.zillabyte.motherbrain.flow.FlowService;
import com.zillabyte.motherbrain.flow.operations.Operation;
import com.zillabyte.motherbrain.universe.Universe;

public class LocalFlowService implements FlowService {

  Map<String, LocalFlowController> _controllers = Maps.newHashMap();
  
  
  @Override
  public synchronized FlowInstance registerApp(App app) {
    try { 
      
      if (_controllers.containsKey(app.getId())) {
        killFlow(_controllers.get(app.getId()).flowInstance());
      }
      FlowInstance inst = new FlowInstance(app);
      LocalFlowController controller = new LocalFlowController(inst);
      _controllers.put(inst.id(), controller);
      
      // Finalize the declaration phase
      for(Operation o : app.getOperations()) {
        o.onSetExpectedFields();
      }
      for(Operation o : app.getOperations()) {
        o.onFinalizeDeclare();
      }
      for(Operation o : app.getOperations()) {
        o.parseFlowGraph();
      }
      
      // Serialize multilangs... 
      Universe.instance().containerFactory().createSerializer().serializeFlow(app);
      
      // Register it back to the api (i.e. share all the meta (nodes, arcs, files) settings... 
      Universe.instance().api().postFlowRegistration(app.getId(), app.getMeta(), app.getFlowConfig().getAuthToken());
      
      // Starts the instances... 
      inst.start();
      controller.start();
      inst.handlePostDeploy();
  
      return inst;
      
    } catch(Exception e) {
      Throwables.propagate(e);
      return null; 
    }
  }

  
  @Override
  public void registerComponent(Component comp) {
    
  }

  
  @Override
  public void init() {
    _controllers.clear();
  }

  
  @Override
  public synchronized void shutDown() {
    for(LocalFlowController controller :  Sets.newHashSet(_controllers.values())) {
      killFlow(controller.flowInstance());
    }
  }

  
  @Override
  public synchronized void killFlow(FlowInstance inst) {
    if (_controllers.containsKey(inst.id())) {
      LocalFlowController controller = _controllers.get(inst.id());
      controller.stop();
      _controllers.remove(inst.id());
    }
  }

}
