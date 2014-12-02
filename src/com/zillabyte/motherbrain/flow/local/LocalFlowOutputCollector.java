package com.zillabyte.motherbrain.flow.local;

import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.javatuples.Pair;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.zillabyte.motherbrain.flow.MapTuple;
import com.zillabyte.motherbrain.flow.collectors.OutputCollector;
import com.zillabyte.motherbrain.flow.collectors.coordinated.ObserveIncomingTupleAction;
import com.zillabyte.motherbrain.flow.graph.Connection;
import com.zillabyte.motherbrain.flow.operations.Operation;
import com.zillabyte.motherbrain.flow.operations.OperationException;
import com.zillabyte.motherbrain.flow.operations.decorators.EmitDecorator;
import com.zillabyte.motherbrain.relational.DefaultStreamException;
import com.zillabyte.motherbrain.relational.UnexpectedFieldException;
import com.zillabyte.motherbrain.top.MotherbrainException;

public class LocalFlowOutputCollector implements OutputCollector {

  private int _localCounter = 0;
  private long _consumed = 0;
  private long _emitted = 0;
  private LocalOperationSlot _slot;
  
  
  
  public LocalFlowOutputCollector(LocalOperationSlot slot) {
    _slot = slot;
  }
  
  
  @Override
  public void emit(String streamName, MapTuple t) throws OperationException {
    this.emitAndGetTasks(streamName, t);
  }

  @Override
  public void emit(MapTuple t) throws OperationException {
    try {
      emit(this.getDefaultStream(), t);
    } catch (DefaultStreamException e) {
      throw new OperationException(_slot.operation(), e);
    }
  }


  
  @Override
  public void onAfterTuplesEmitted() throws OperationException {
  }
  

  @Override
  public void resetCounter() {
    _localCounter = 0;
  }

  @Override
  public long getCounter() {
    return _localCounter;
  }

  @Override
  public Operation getOperation() {
    return _slot.operation();
  }

  @Override
  public void configure(Object context) {
  }

  @Override
  public String getDefaultStream() throws DefaultStreamException {
    return _slot.operation().defaultStream();
  }

  @Override
  public void emitDirect(Integer taskId, String streamId, List<?> rawTuple) {
    _slot.controller().emitDirect(_slot.task(), taskId, streamId, rawTuple);
  }

  @Override
  public List<Integer> emitAndGetTasks(String streamName, MapTuple t) throws OperationException {
    t = handlePreEmit(streamName, t);    
    List<Integer> tasks = _slot.controller().emitToStream(streamName, t, this._slot.task());
    return tasks;
  }

  @Override
  public Integer getThisTask(Object context) {
    return _slot.task();
  }

  @Override
  public void constructTaskOperationInfo(Object context) {
  }

  @Override
  public SetMultimap<String, Integer> getTaskOperationMap() {
    SetMultimap<String, Integer> ret = HashMultimap.create();
    for(LocalOperationSlot s : _slot.controller().allSlots()) {
      ret.put(s.operation().namespaceName(), s.task());
    }
    return ret;
  }

  @Override
  public Set<Integer> getAllTasks() {
    Set<Integer> set = Sets.newHashSet();
    for(LocalOperationSlot slot : _slot.controller().allSlots()) {
      set.add(slot.task());
    }
    return set;
  }

  @Override
  public Set<Integer> getAdjacentDownStreamTasks() {
    Set<Integer> set = Sets.newHashSet(); 
    for(Pair<Connection, LocalOperationSlot> p : _slot.controller().getDownstreamSlots(_slot)) {
      set.add(p.getValue1().task());
    }
    return set;
  }

  @Override
  public Set<Integer> getAdjacentUpStreamNonLoopTasks() {
    Set<Integer> set = Sets.newHashSet(); 
    for(Pair<Connection, LocalOperationSlot> p : _slot.controller().getUpstreamSlots(_slot)) {
      if (p.getValue0().loopBack() == false) {
        set.add(p.getValue1().task());
      }
    }
    return set;
  }

  @Override
  public ObserveIncomingTupleAction observePostQueuedCoordTuple(Object tuple, Integer sourceTask) throws OperationException {
    if (tuple instanceof MapTuple) {
      this.observeIncomingTuple((MapTuple)tuple);
    }
    return ObserveIncomingTupleAction.CONTINUE;
  }
  
  @Override
  public ObserveIncomingTupleAction observePreQueuedCoordTuple(Object tuple, Integer originTask) throws OperationException {
    return ObserveIncomingTupleAction.CONTINUE;
  }


  @Override
  public long getConsumeCount() {
    return _consumed;
  }

  @Override
  public long getEmitCount() {
    return _emitted;
  }

  @Override
  public long getAckCount() {
    return 0;
  }

  @Override
  public long getFailCount() {
    return 0;
  }

  @Override
  public long getCoordEmitCount() {
    return 0;
  }

  @Override
  public long getCoordConsumeCount() {
    return 0;
  }

  @Override
  public void handleChecks() {
  }

  @Override
  public boolean inPressureState() {
    return false;
  }

  
  
  public Operation operation() {
    return this._slot.operation();
  }
  

  
  
  
  /****************************************************************************
   * BELOW: just copied from StromOutpuCollector... TODO: DRY it up. 
   ****************************************************************************/
  
  private MapTuple _inputTuple;


  private MapTuple handlePreEmit(String streamName, MapTuple t) throws OperationException {
    
    // Pre-processors
    t = handlePostEmitDecorators(streamName, t);
    t = handleMerge(t);

    // Sanity
    ensureExpected(streamName, t);
    return t;
  }

  

  
  @Override
  public void observeIncomingTuple(MapTuple tuple) {
    _inputTuple = tuple;
  }

  
  
  protected void ensureExpected(String streamName, MapTuple t) throws OperationException {
    // _log.info("ensureExpected: streamName=" + streamName + " mapTuple=" + t);
    if (operation().outputStreams().contains(streamName) == false) {
      this.operation().getTopFlow().graph().debug();
      throw new OperationException(this.operation(), new UnexpectedFieldException().setUserMessage("Emitted to an unexpected stream: '" + streamName + "'. Expected: " + this.operation().outputStreams()));
    }
    for (String field : this.operation().getExpectedFields(streamName)) {
      if (t.values().containsKey(field) == false) {
        throw new OperationException(this.operation(), new UnexpectedFieldException().setUserMessage("The tuple: '" + t.toString() + "' does not contain expected field: '" + field + "'"));
      }
    }
  }


  private MapTuple handlePostEmitDecorators(String stream, MapTuple t) throws OperationException {
    try {
      for(EmitDecorator dec : operation().emitDecorators(stream)) {
        t = dec.execute(t);
      }
      return t;
    } catch(MotherbrainException e) {
      throw new OperationException(operation(), e);
    }
  }


  
  private MapTuple handleMerge(MapTuple t) throws OperationException {
    
    // Sanity.. 
    if (operation().getOperationShouldMerge() && _inputTuple == null)
      throw new OperationException(operation(), "the input tuple is null for a merge!");

    // If we need to merge the input tuple into the output tuple...
    if (_inputTuple != null) {
      final Set<Entry<String, Object>> inputs = _inputTuple.values().entrySet();

      // Put all of the input fields in the output tuple
      for (final Entry<String, Object> entry : inputs) {
        final String field = entry.getKey();
        final Object value = entry.getValue();
        
        if (operation().getOperationShouldMerge() && !t.containsValueKey(field)) {
          // 'pop' this field off the carry 
          t.add(field, value);
        }
        
        if (field.contains(Operation.COMPONENT_CARRY_FIELD_PREFIX)) {
          // Carry the tuple to the next operation.... 
          if (Operation.NONLINEAR_OPS.contains(operation().type()))
            throw new OperationException(operation(), "input field merge requested for component containing aggregation, merges are only allowed on components with only each and filter operations (this includes nested component operations).");
          t.add(field, value);
        }
      }
    }
    
    // Done 
    return t;
  }


  @Override
  public Object getCurrentBatch() {
    return "__local_batch";
  }
  

  
}
