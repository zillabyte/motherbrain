package com.zillabyte.motherbrain.flow.collectors;

import java.util.List;
import java.util.Set;

import com.google.common.collect.SetMultimap;
import com.zillabyte.motherbrain.flow.MapTuple;
import com.zillabyte.motherbrain.flow.collectors.coordinated.ObserveIncomingTupleAction;
import com.zillabyte.motherbrain.flow.operations.LoopException;
import com.zillabyte.motherbrain.flow.operations.Operation;

public interface OutputCollector {

  public void emit(String streamName, MapTuple t) throws LoopException;
  public void emit(MapTuple t) throws LoopException;
  
  public void observeIncomingTuple(MapTuple tuple);
  public void onAfterTuplesEmitted() throws LoopException;
  
  public void resetCounter();
  public long getCounter();
  
  public Operation getOperation();
  
  public void configure(Object context);
  
  public String getDefaultStream();
  

  public void emitDirect(Integer taskId, String streamId, List<?> rawTuple);
 
  public List<Integer> emitAndGetTasks(String streamName, MapTuple t) throws LoopException;

  public Integer getThisTask(Object context);

  public void constructTaskOperationInfo(Object context);
  public SetMultimap<String, Integer> getTaskOperationMap();
  public Set<Integer> getAllTasks();
  public Set<Integer> getAdjacentDownStreamTasks();
  public Set<Integer> getAdjacentUpStreamNonLoopTasks();
  
  public ObserveIncomingTupleAction observePreQueuedCoordTuple(Object tuple, Integer originTask) throws LoopException;
  public ObserveIncomingTupleAction observePostQueuedCoordTuple(Object tuple, Integer sourceTask) throws LoopException;
  
  public long getConsumeCount();
  public long getEmitCount();
  public long getAckCount();
  public long getFailCount();
  public long getCoordEmitCount();
  public long getCoordConsumeCount();
  
  public void handleChecks();
  public boolean inPressureState();
  
  public Object getCurrentBatch(); 
  
  
  
}
