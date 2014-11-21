package com.zillabyte.motherbrain.flow;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import net.sf.json.JSONObject;

import org.eclipse.jdt.annotation.NonNull;
import org.eclipse.jdt.annotation.NonNullByDefault;

import com.zillabyte.motherbrain.flow.components.ComponentOutput;
import com.zillabyte.motherbrain.flow.config.FlowConfig;
import com.zillabyte.motherbrain.flow.graph.FlowGraph;
import com.zillabyte.motherbrain.flow.operations.Operation;
import com.zillabyte.motherbrain.flow.operations.OperationLogger;
import com.zillabyte.motherbrain.universe.Config;

public abstract class Flow implements Serializable {
  
  private static final long serialVersionUID = 2743277586672733175L;
  
  private String _name;
  private String _id;
  private String _runId;
  protected OperationLogger _logger = new OperationLogger.noOp();
  private boolean _isCoordinated = false;
  private JSONObject _meta = new JSONObject();  // cached copy of 'zillabyte info' & other data... 
  private Flow _parentFlow = null;
  
  protected FlowGraph _graph = new FlowGraph();
  protected Map<String, Integer> _nestedComponents = new HashMap<>();
  protected  FlowConfig _flowConfig;
  
  

  protected Flow(final String id, final String name, FlowConfig flowConfig) {
    this._id = id;
    this._name = name;
    this._runId = Integer.toHexString((int) (Math.random()*10000));
    this._flowConfig = flowConfig;
  }

  public void addComponent(String id) {
    Integer c = _nestedComponents.get(id);
    if(c == null) {
      _nestedComponents.put(id, Integer.valueOf(0));
    } else {
      _nestedComponents.put(id, Integer.valueOf(c.intValue() + 1));
    }
  }
  
  public void setLogger(OperationLogger logger) {
    _logger = logger;
  }
  
  public OperationLogger getLogger() {
    return _logger;
  }
  
  public final @NonNull String getName() {
    return _name;
  }

  
  public final @NonNull String getId() {
    return _id;
  }
  
  
  public Collection<Operation> getOperations() {
    return _graph.allOperations();
  }

  public int getExpectedNumberOfNodes() {
    return this.getOperations().size();
  }
  
  public Operation getOperation(final @NonNull String id) {
    return this._graph.getById(id);
  }


  public FlowGraph graph() {
    return _graph;
  }
  
 


  public abstract @NonNullByDefault StreamBuilder createStream(ComponentOutput producer);

  public boolean isCoordinated() {
    return _isCoordinated || Config.getOrDefault("storm.coordinated", Boolean.TRUE).booleanValue();
  }

  public FlowConfig getFlowConfig() {
    return _flowConfig;
  }
  
  public String flowStateKey() {
    return "flows/" + getId() + "/cycle_" + getVersion();
  }
  
  public String operationStateKey(Operation op) {
    return flowStateKey() + "/operations/" + op.namespaceName();
  }
  

  public void setMeta(JSONObject meta) {
    _meta.putAll(meta);
  }
  
  public JSONObject getMeta() {
    return _meta;
  }
  

  public void addMeta(String key, Object value) {
    _meta.put(key, value);
  }
  

  public Flow getTopFlow() {
    Flow f = this;
    while(f.getParentFlow() != null) {
      f = f.getParentFlow();
    }
    return f;
  }
  
  public Flow getParentFlow() {
    return _parentFlow ;
  }
  
  public void setParentFlow(Flow f) {
    _parentFlow = f;
  }

  public Integer getVersion() {
    return _flowConfig.getFlowVersion();
  }

}
