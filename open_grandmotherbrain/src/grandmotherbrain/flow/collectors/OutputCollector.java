package grandmotherbrain.flow.collectors;

import grandmotherbrain.flow.MapTuple;
import grandmotherbrain.flow.collectors.coordinated.ObserveIncomingTupleAction;
import grandmotherbrain.flow.operations.Operation;
import grandmotherbrain.flow.operations.OperationException;
import grandmotherbrain.relational.DefaultStreamException;

import java.util.List;
import java.util.Set;

import com.google.common.collect.SetMultimap;

public interface OutputCollector {

  public void emit(String streamName, MapTuple t) throws OperationException;
  public void emit(MapTuple t) throws OperationException;
  
  public void observeIncomingTuple(MapTuple tuple);
  public void onAfterTuplesEmitted() throws OperationException;
  
  public void resetCounter();
  public long getCounter();
  
  public Operation getOperation();
  
  public void configure(Object context);
  
  public String getDefaultStream() throws DefaultStreamException;
  

  public void emitDirect(Integer taskId, String streamId, List<?> rawTuple);
 
  public List<Integer> emitAndGetTasks(String streamName, MapTuple t) throws OperationException;

  public Integer getThisTask(Object context);

  public void constructTaskOperationInfo(Object context);
  public SetMultimap<String, Integer> getTaskOperationMap();
  public Set<Integer> getAllTasks();
  public Set<Integer> getAdjacentDownStreamTasks();
  public Set<Integer> getAdjacentUpStreamNonLoopTasks();
  
  public ObserveIncomingTupleAction observePreQueuedCoordTuple(Object tuple, Integer originTask) throws OperationException;
  public ObserveIncomingTupleAction observePostQueuedCoordTuple(Object tuple, Integer sourceTask) throws OperationException;
  
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
