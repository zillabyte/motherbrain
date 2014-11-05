package com.zillabyte.motherbrain.flow;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.eclipse.jdt.annotation.NonNullByDefault;

import com.google.common.collect.Lists;
import com.google.monitoring.runtime.instrumentation.common.com.google.common.collect.Maps;
import com.zillabyte.motherbrain.flow.heartbeats.Heartbeat;
import com.zillabyte.motherbrain.flow.operations.Operation;
import com.zillabyte.motherbrain.universe.Config;
import com.zillabyte.motherbrain.utils.Utils;

@NonNullByDefault
@SuppressWarnings("all")
public class FlowInstanceSetBuilder {

  private static Logger _log = Utils.getLogger(FlowInstanceSetBuilder.class);
  
  /*****
   * 
   *
   */
  public static final class Mock extends FlowInstanceSetBuilder {
    
    public Mock() {
      super();
    }

    /**
     * @param instanceId  
     */
    public Mock addInstance(String type, String operationId, String instanceId, String state, Long lastHeartbeat) {
//      final HashMap<String, String> stats = new HashMap<>();
//      final Map<String, Object> info = new ConcurrentHashMap<>();
      final Map<String, Object> inst = new ConcurrentHashMap<>();
      inst.put("name", operationId);
      inst.put("type", type);
      inst.put("instance_name", instanceId);

//      final OperationInstanceState<FlowInstance,ConcurrentHashMap<String,Object>> instState =
//          OperationInstanceState.makeOperationInstanceState(state, stats, info);
//      instState.updated(lastHeartbeat);
//      _instances.add(instState);
      inst.put("state", state);
      inst.put("last_heartbeat", lastHeartbeat);
      inst.put("last_update_time", System.currentTimeMillis());
      
      _instances.add(inst);
      return this;
    }
    
    /***
     * 
     */
    public Mock addInstance(String type, String operationId, String instanceId, String state) {
      return addInstance(type, operationId, instanceId, state, System.currentTimeMillis());
    }
    
  }
  
  

  public static final FlowInstanceSetBuilder EMPTY_SET = new FlowInstanceSetBuilder(Collections.EMPTY_LIST);
  
  private Long KILL_INTERVAL_MS = Config.getOrDefault("heartbeat.kill.interval", Heartbeat.DEFAULT_KILL_INTERVAL_MS)+1000;
  private Long ACTIVITY_KILL_INTERVAL_MS = Config.getOrDefault("operation.activity.timeout", Operation.ACTIVE_OPERATION_TIMEOUT_DEFAULT)+1000;
  
  final protected Collection<Map<String, Object>> _instances;  

  public FlowInstanceSetBuilder(final Collection<Map<String,Object>> instances) {
    _instances = instances;
  }
  
  public FlowInstanceSetBuilder() {
    _instances = new ArrayList<>();
  }
  
  /***
   * 
   * @param instances
   */
  public static FlowInstanceSetBuilder buildFromOperationInstanceStates (FlowOperationInstanceCollection instances) {
    
    final Collection<Map<String, Object>> hashInstances = new ArrayList<>();
    
    for(FlowOperationInstance h : instances) {
      final Map<String, Object> info = h.getInfo();
      final Map<String,String> stats = h.getStats();
      final Map<String, Object> inst = Maps.newHashMap();

      for(String key : info.keySet()) {
        inst.put(key, info.get(key));
      }
      inst.put("state", h.getState());
      inst.put("last_update_time", h.lastUpdateTime());
      if(stats.containsKey("last_heartbeat")) inst.put("last_heartbeat", stats.get("last_heartbeat"));
      
      hashInstances.add(inst);
    }

    return new FlowInstanceSetBuilder(hashInstances);
  }
  
  
  
  
  public Collection<Map<String, Object>> instances() {
    return _instances;
  }
  
  
  

  /***
   * 
   * @param states
   */
  public FlowInstanceSetBuilder inState(String... states) {

    // Init
    final Collection<Map<String, Object>> newList = new LinkedList<>();
    
    for(Map<String, Object> h : _instances) {
      
      // Init
      String opState = (String) h.get("state");
      boolean found = false;
      
      for(String target : states) {
        if (opState.equals(target)) {
          found = true;
          break;
        }
      }
      
      if (found) {
        // This op is the target state, so add it to the list
        newList.add(h);
      }
    }
    
    return new FlowInstanceSetBuilder(newList);
  }
  
  
  
  /***
   * 
   * @param states
   */
  public FlowInstanceSetBuilder notInState(String... states) {
    
    // Init
    final Collection<Map<String, Object>> newList = new LinkedList<>();
    
    for(Map<String, Object> h : _instances) {
      
      // Init 
      String opState = (String) h.get("state");
      boolean found = false;
      
      for(String target : states) {
        if (opState.equals(target)) {
          found = true;
          break;
        }
      }
      
      if (found == false) {
        // This op is not the target state, so add it to the list
        newList.add(h);
      }
    }
    
    return new FlowInstanceSetBuilder(newList);
  }
  
  
  
  /****
   * 
   * @param targetTypes
   */
  public FlowInstanceSetBuilder ofType(String... targetTypes) {
    final Collection<Map<String, Object>> newList = new LinkedList<>();
    for(Map<String, Object> h : _instances) {
      synchronized (h) {
        final Object opType = h.get("type");
        if (opType == null) {
          continue;
        }
        boolean found = false;
        for(final String target : targetTypes) {
          if (opType.equals(target)) {
            found = true;
            break;
          }
        }
        if (found) {
          /*
           * This op type was not found, so add it to the list.
           */
          newList.add(h);
        }
      }
    }
    return new FlowInstanceSetBuilder(newList);
  }

  /***
   * 
   * @param targetTypes
   */
  public FlowInstanceSetBuilder ofOperation(Operation... targetTypes) {
    return new FlowInstanceSetBuilder((getOperationInstances(targetTypes)));
  }

  public FlowInstanceSetBuilder ofOperation(String... targetOperations) {
    return new FlowInstanceSetBuilder((getOperationInstances(targetOperations)));
  }

  private List<Map<String, Object>> getOperationInstances(String... operations) {
    final LinkedList<Map<String, Object>> ret = new LinkedList<>();
    for(Map<String, Object> h : _instances) {
      synchronized (h) {
        final Object name = h.get("name");
        if (name == null) {
          continue;
        }
        for(String o : operations) {
          if (o.equals(name)) {
            ret.add(h);
          }
        }
      }
    }
    return ret;
  }

  /***
   * 
   * @param operations
   */
  private List<Map<String, Object>> getOperationInstances(Operation... operations) {
    final LinkedList<Map<String, Object>> ret = new LinkedList<>();
    for(Map<String, Object> h : _instances) {
      synchronized (h) {
        final Object name = h.get("name");
        if (name == null) {
          continue;
        }
        for(Operation o : operations) {
          if (o.namespaceName().equals(name)) {
            ret.add(h);
          }
        }
      }
    }
    return ret;
  }
  
  
  
  /***
   * 
   * @param targetTypes
   */
  public FlowInstanceSetBuilder notOfType(String... targetTypes) {

    // Init
    final Collection<Map<String, Object>> newList = new LinkedList<>();
    
    for(Map<String, Object> h : _instances) {
      synchronized (h) {
        final Object opType = h.get("type");
        if (opType == null) {
          continue;
        }
        boolean found = false;
        for(final String target : targetTypes) {
          if (opType.equals(target)) {
            found = true;
            break;
          }
        }
        if (found == false) {
          /*
           * This op type was not found, so add it to the list.
           */
          newList.add(h);
        }
      }
    }
    return new FlowInstanceSetBuilder(newList);
    
  }
 
  
  
  public boolean isEmpty() {
    return this._instances.isEmpty();
  }
  
  public boolean isNotEmpty() {
    return !isEmpty();
  }


  /***
   * 
   * @param states
   */
  public boolean anyInState(String... states) {
    FlowInstanceSetBuilder r = this.inState(states);
    return r._instances.size() > 0;
  }
  
  
  /****
   * 
   * @param states
   */
  public boolean allInState(String... states) {
    int preSize = this._instances.size();
    FlowInstanceSetBuilder r = this.inState(states);
    return r._instances.size() == preSize && preSize > 0;
  }


  public FlowInstanceSetBuilder sources() {
    return this.ofType("source");
  }
  
  public FlowInstanceSetBuilder nonSources() {
    return this.notOfType("source");
  }


  /***
   * 
   * @param ops
   */
  public FlowInstanceSetBuilder ofOperation(final List<Operation> ops) {
    final Operation[] arrayOps = ops.toArray(new Operation[] {});
    /*
     * True by contract.
     */
    assert (arrayOps != null);
    return this.ofOperation(arrayOps);
  }


  
  /****
   * 
   */
  public void debugStates() {
    System.err.println("----");
    for(Map<String, Object> h : _instances) {
      final String opState = (String) h.get("state");
      synchronized (h) {
        final Object name = h.get("name");
        System.err.println(name == null? "null" : name + ": " + opState);
      }
    }
  }


  public Set<String> operationIds() {
    final Set<String> ids = new HashSet<>();
    for(Map<String, Object> h : _instances) {
      synchronized (h) {
        final Object name = h.get("name");
//        _log.info(_uuid + " in operationIds, the set info is: "+info);
        if (name != null) {
          ids.add(name.toString());
        }
      }
    }
    return ids;
  }


  public int size() {
    return this._instances.size();
  }




  /****
   * 
   * @return
   */
  public FlowInstanceSetBuilder withAliveHeartbeats() {
    
    final Collection<Map<String, Object>> newList = Lists.newLinkedList(); 
    
    for(Map<String, Object> instanceState : _instances) {
      final String state = (String) instanceState.get("state");
      synchronized (instanceState) {
        final Object opId = instanceState.get("instance_name");
//        _log.info(_uuid+" Set info: "+info);
        final Long lastHeartbeat = (Long) instanceState.get("last_heartbeat");
        if (opId == null) continue;
        if(lastHeartbeat == null) {
          // If we don't have heartbeat info yet, sanity check last state update time
          if(System.currentTimeMillis() - ((Long) instanceState.get("last_update_time")) < ACTIVITY_KILL_INTERVAL_MS) {
            newList.add(instanceState);
          } else {
            _log.debug(instanceState.get("name") + " [" + opId + "] has not had activity for more than "+ACTIVITY_KILL_INTERVAL_MS);
          }
        } else {
          // Use heartbeat info to determine if instance is dead
          if ( System.currentTimeMillis() - lastHeartbeat < KILL_INTERVAL_MS) {
            newList.add(instanceState);
          } else {
            _log.debug(instanceState.get("name") + " [" + opId + "]'s last heartbeat was at " + lastHeartbeat+". That was more than " + KILL_INTERVAL_MS +" ago.");
          }
        }
      }
    }
    
    return new FlowInstanceSetBuilder(newList);
  }

  
  
  /***
   * 
   * @return
   */
  public FlowInstanceSetBuilder withDeadHeartbeats() {
    
    final Collection<Map<String, Object>> newList = Lists.newLinkedList(); 
    
    for(Map<String, Object> instanceState : _instances) {
      final String state = (String) instanceState.get("state");
      synchronized (instanceState) {
        final Object opId = instanceState.get("instance_name");
        final String lastHeartbeat = (String) instanceState.get("last_heartbeat");
        if (opId == null) continue;
        if(lastHeartbeat == null) {
          // if we don't have heartbeat info yet, sanity check last state update time
          if (System.currentTimeMillis() - Long.valueOf((String) instanceState.get("last_update_time")) >= ACTIVITY_KILL_INTERVAL_MS) newList.add(instanceState);
        } else {
          // Use heartbeat info to determine if instance is dead
          final Long longHeartbeat = Long.valueOf(lastHeartbeat);
          if (System.currentTimeMillis() - longHeartbeat >= KILL_INTERVAL_MS) newList.add(instanceState);
        }
      }
    }
    
    return new FlowInstanceSetBuilder(newList);
  }




  /***
   * If all the instances of a given operation are dead, then return empty set. 
   * @return
   */
  public FlowInstanceSetBuilder assertAtLeastOneInstanceAliveFromEachOperation() {
//    _log.info(_uuid+" All operations: "+this.operationIds());
//    _log.info(_uuid+" Live ops: "+this.withAliveHeartbeats().operationIds());
    if (this.withAliveHeartbeats().operationIds().equals( this.operationIds() )) {
      return this;
    } else {
      return EMPTY_SET;
    }   
  }
  
  
  public boolean allOperationsAlive() {
    if(this.assertAtLeastOneInstanceAliveFromEachOperation() == this) {
      return true;
    }
    return false;
  }

  public Set<String> getNotAliveOperationNames() {
    Set<String> allNotAliveOps = this.operationIds();
    for(String op : this.withAliveHeartbeats().operationIds()) {
      allNotAliveOps.remove(op);
    }
    return allNotAliveOps;  //any remaining ops are not online yet..
  }
  
  
  
}
