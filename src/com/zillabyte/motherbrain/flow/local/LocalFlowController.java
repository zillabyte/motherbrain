package com.zillabyte.motherbrain.flow.local;

import java.util.Collection;
import java.util.List;

import org.apache.log4j.Logger;
import org.javatuples.Pair;

import com.google.common.collect.ArrayListMultimap;
import com.google.monitoring.runtime.instrumentation.common.com.google.common.collect.Lists;
import com.zillabyte.motherbrain.flow.App;
import com.zillabyte.motherbrain.flow.FlowInstance;
import com.zillabyte.motherbrain.flow.MapTuple;
import com.zillabyte.motherbrain.flow.graph.Connection;
import com.zillabyte.motherbrain.flow.graph.FlowGraph;
import com.zillabyte.motherbrain.flow.operations.Operation;
import com.zillabyte.motherbrain.utils.Utils;

public class LocalFlowController {

  private FlowInstance _flowInstance;
  private App _flow;
  private ArrayListMultimap<String, LocalOperationSlot> _operationInstanceMap = ArrayListMultimap.create();
  private final int _maxParallelism = 1;
  private boolean _exitOnError = true;
  private boolean _running = false;
  private static Logger _log = Utils.getLogger(LocalFlowController.class);

  public LocalFlowController(FlowInstance inst) {
    _flowInstance = inst;
    _flow = inst.flow();
  }
  
  
  public synchronized void start() {
    buildSlots();
    for(LocalOperationSlot o : _operationInstanceMap.values()) {
      o.prepare();
    }
    for(LocalOperationSlot o : _operationInstanceMap.values()) {
      o.start();
    }
    _running = true;
  }
  
  public synchronized void stop() {
    _running = false;
    for(LocalOperationSlot o : _operationInstanceMap.values()) {
      o.stop();
    }
    _operationInstanceMap.clear();
  }

  public Collection<LocalOperationSlot> getSlotsFor(Operation op) {
    return _operationInstanceMap.get(op.namespaceName());
  }
  
  public LocalOperationSlot getOneSlotFor(Operation op) {
    List<LocalOperationSlot> c = _operationInstanceMap.get(op.namespaceName());
    if (c.size() != 1) throw new IllegalStateException("unexpected number of slots");
    return c.get(0);
  }
  
  public Collection<Pair<Connection, LocalOperationSlot>> getDownstreamSlots(LocalOperationSlot origin) {
    List<Connection> cs = graph().connectionsFrom(origin.operation());
    List<Pair<Connection, LocalOperationSlot>> slots = Lists.newLinkedList();
    for(Connection c : cs) {
      slots.add(new Pair<>(c, getOneSlotFor(c.dest())));
    }
    return slots;
  }
  
  public Collection<Pair<Connection, LocalOperationSlot>> getUpstreamSlots(LocalOperationSlot origin) {
    List<Connection> cs = graph().connectionsTo(origin.operation());
    List<Pair<Connection, LocalOperationSlot>> slots = Lists.newLinkedList();
    for(Connection c : cs) {
      slots.add(new Pair<>(c, getOneSlotFor(c.source())));
    }
    return slots;
  }

  public List<Integer> emitToStream(String streamName, MapTuple tuple, Integer sourceTask) {
    
    // Find the operation this stream goes to.. 
    List<Connection> connections = graph().getConnectionByStream(streamName);
    if (connections == null || connections.isEmpty()) throw new RuntimeException("Stream not found: " + streamName);
    
    List<Integer> ret = Lists.newLinkedList();
    
    for(Connection connection : connections) {
      String destOpId = connection.destId();

      // Find all the instances of the dest..
      List<LocalOperationSlot> slots = _operationInstanceMap.get(destOpId);
      if (slots.size() == 0) throw new RuntimeException("no slots for operation: " + destOpId);

      // How shall we route this tuple?
      if (slots.size() == 1) {
        LocalOperationSlot slot = slots.get(0);
        slot.enqueueTuple(sourceTask, streamName, tuple);
        ret.add(slot.task());
      } else {
        Utils.TODO("implement round-robin and hash-based routing");
      }
    }
    
    return ret; // TODO: return list of tasks
  }
  
  
  private FlowGraph graph() {
    return this._flow.graph();
  }
  
  
  private void buildSlots() {
    
    // Build the slots...
    Collection<Operation> ops = _flow.graph().allOperations();
    int count = 0; 
    for(Operation original : ops) {     
      // Parallelism... 
      int parallelism = Math.min(_maxParallelism, original.getMaxParallelism());
      original.setActualParallelism(parallelism);
      
      // Clone it... 
      byte[] serialized = Utils.serialize(original);
      
      // create an instance for each parallel
      for (int i=0;i<parallelism;i++) {
        Operation cloned = (Operation)Utils.deserialize(serialized);
        LocalOperationSlot slot = new LocalOperationSlot(cloned, count++, this);
        _operationInstanceMap.put(original.namespaceName(), slot);
      }
    }
  }
  
  public FlowInstance flowInstance() {
    return this._flowInstance;
  }
  
  
  public Collection<LocalOperationSlot> allSlots() {
    return this._operationInstanceMap.values();
  }

  void handleSlotError(LocalOperationSlot slot, Exception e) {
    if (this._exitOnError) {
      e.printStackTrace();
      _log.error("Exiting because _exitOnError is set. Slot '" + slot + "' had exception: " + e.getMessage());
      System.exit(1);
    } else {
      Utils.TODO("how to handle error?");
    }
  }

  public LocalOperationSlot getSlotByTask(Integer task) {
    for(LocalOperationSlot o : _operationInstanceMap.values()) {
      if (o.task() == task) {
        return o;
      }
    }
    return null;
  }

  public void emitDirect(Integer sourceTask, Integer destTask, String stream, Object tuple) {
    if (_running) {
      LocalOperationSlot slot = getSlotByTask(destTask);
      if (slot == null)
        throw new NullPointerException("could not find slot " + destTask);
      slot.enqueueTuple(sourceTask, stream, tuple);
    } else {
      // maybe we're just shutting down... 
      _log.warn("tuple emitted while slot is not running: " + tuple);
    }
  }
  
}
