package com.zillabyte.motherbrain.flow.buffer.mock;

import java.util.List;

import com.google.common.collect.Lists;
import com.google.monitoring.runtime.instrumentation.common.com.google.common.base.Throwables;
import com.zillabyte.motherbrain.api.RelationsHelper;
import com.zillabyte.motherbrain.flow.MapTuple;
import com.zillabyte.motherbrain.flow.buffer.BufferProducer;
import com.zillabyte.motherbrain.flow.buffer.SinkToBuffer;
import com.zillabyte.motherbrain.relational.ColumnDef;
import com.zillabyte.motherbrain.universe.Universe;

public class LocalDevBufferProducer implements BufferProducer {

  private static final int BUFFER_FLUSH_LIMIT = 100;
  private static final long BUFFER_BYTE_LIMIT = 50_000;
  private SinkToBuffer _operation;
  private List<MapTuple> _buffer = Lists.newLinkedList();
  private long _currentBufferByteSize = 0L;
  private boolean _pushedSettings = false;
  private String _authToken;

  public LocalDevBufferProducer(SinkToBuffer operation) {
    _operation = operation;
  }

  @Override
  public synchronized void pushTuple(MapTuple t) {
    _buffer.add(t);
    _currentBufferByteSize += t.getApproxMemSize();
    if (_currentBufferByteSize > BUFFER_BYTE_LIMIT) {
      flush();
    }
    if (_buffer.size() > BUFFER_FLUSH_LIMIT) {
      flush();
    }
  }

  public synchronized void flush() {
    try {
      
      // First... create the relation
      if (_buffer.size() > 0) {
        
        // Push settings? 
        if (_pushedSettings == false) {
          RelationsHelper.instance().postRelationConfig(
              _operation.getTopFlow().getFlowConfig(), 
              _operation.getRelation().name(), 
              _operation.getRelation().valueColumns().toArray(new ColumnDef[] {}));
          _pushedSettings = false;
        }
            
        // Now, flush it...
        Universe.instance().api().appendRelation(
            _operation.getRelation().name(),
            _buffer,
            _operation.getTopFlow().getFlowConfig().getAuthToken()
            );
      }
      
    } catch(Exception e) {
      Throwables.propagate(e);
    } finally {
      _buffer.clear();
      _currentBufferByteSize = 0L;
    }
  }
  
}
