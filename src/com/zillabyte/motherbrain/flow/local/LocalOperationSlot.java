package com.zillabyte.motherbrain.flow.local;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.codehaus.plexus.util.ExceptionUtils;
import org.javatuples.Triplet;

import com.google.monitoring.runtime.instrumentation.common.com.google.common.base.Throwables;
import com.zillabyte.motherbrain.flow.MapTuple;
import com.zillabyte.motherbrain.flow.collectors.OutputCollector;
import com.zillabyte.motherbrain.flow.collectors.coordinated.CoordinatedOutputCollector;
import com.zillabyte.motherbrain.flow.collectors.coordinated.ObserveIncomingTupleAction;
import com.zillabyte.motherbrain.flow.operations.Join;
import com.zillabyte.motherbrain.flow.operations.Operation;
import com.zillabyte.motherbrain.flow.operations.ProcessableOperation;
import com.zillabyte.motherbrain.flow.operations.Source;
import com.zillabyte.motherbrain.utils.Utils;

public class LocalOperationSlot {

  private Operation _operation;
  private OutputCollector _collector;
  private Integer _taskId = -1;
  private LinkedBlockingQueue<Triplet<Integer, String, Object>> _queue = new LinkedBlockingQueue<>();
  private LocalFlowController _controller;
  private Future<Void> _future;
  private static Logger _log = Utils.getLogger(LocalOperationSlot.class);

  
  public LocalOperationSlot(Operation o, Integer taskId, LocalFlowController controller) {
    _operation = o;
    _taskId = taskId;
    _collector = new CoordinatedOutputCollector(new LocalFlowOutputCollector(this));
    _controller = controller;
  }
  
  public void prepare() {
    _collector.configure(null);
    _operation.handlePrepare(_collector);
  }
  
  public boolean isRunning() {
    if (_future != null) {
      return !_future.isDone();
    }
    return false;
  }
  
  public void start() {
    if (_future != null) throw new RuntimeException("Future already exists!");
    _future = Utils.run(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        
        try { 
          while(true) {
            
            // Exit check.. 
            if (Thread.interrupted()) break;
            
            // Source emit check... 
            if (_operation instanceof Source) {
              ((Source)_operation).handleNextTuple(_collector);
            }
            
            // Incoming tuple check... 
            Triplet<Integer, String, Object> triplet = _queue.poll(1, TimeUnit.MILLISECONDS);
            if (triplet == null) continue;
            debug("popping local tuple: " + triplet.getValue2());
            
            // INIT  
            Integer fromTask = triplet.getValue0();
            String onStream = triplet.getValue1();
            Object rawTuple = triplet.getValue2();
            Object tuple = null;
            
            if (rawTuple instanceof List) {
              tuple = ((List)rawTuple).get(0);
            } else {
              tuple = rawTuple;
            }
            
            if (_collector.observePreQueuedCoordTuple(tuple, fromTask) == ObserveIncomingTupleAction.STOP) 
              continue;
            if (_collector.observePostQueuedCoordTuple(tuple, fromTask) == ObserveIncomingTupleAction.STOP)
              continue;
            
            // Process the next item... 
            if (_operation instanceof ProcessableOperation) {
              if (_operation instanceof Join) {
                ((Join) _operation).handleProcess((MapTuple) tuple, onStream, _collector);
              } else {
                ((ProcessableOperation)_operation).handleProcess((MapTuple) tuple, _collector);
              }
            } else {
              throw new RuntimeException("Unexpected operation type: " + _operation);
            }
            
            _collector.onAfterTuplesEmitted(); 
            
          }
        } catch(Exception e) {
          debug(ExceptionUtils.getFullStackTrace(e));
          _log.error(e.getMessage());
          _controller.handleSlotError(LocalOperationSlot.this, e);
          Throwables.propagate(e);
        }
        
        return null;
      }
    });
  }
  
  public void stop() {
    _operation.cleanup();
    if (_future != null) {
      _future.cancel(true);
      _future = null;
    }
    this._queue.clear();
  }
  
  public void enqueueTuple(Integer sourceTask, String stream, Object tuple) {
    debug("queueing local tuple: " + tuple);
    _queue.add(new Triplet<>(sourceTask, stream, tuple));
  }

  public Integer task() {
    return this._taskId;
  }

  public Operation operation() {
    return this._operation;
  }

  public LocalFlowController controller() {
    return this._controller;
  }
  
  private void debug(String s) {
    // System.err.println(s);
  }
}
