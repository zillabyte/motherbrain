package com.zillabyte.motherbrain.flow.buffer.mock;

import java.util.List;

import org.apache.log4j.Logger;

import com.google.common.collect.Lists;
import com.zillabyte.motherbrain.api.APIException;
import com.zillabyte.motherbrain.flow.MapTuple;
import com.zillabyte.motherbrain.flow.buffer.BufferProducer;
import com.zillabyte.motherbrain.flow.buffer.SinkToBuffer;
import com.zillabyte.motherbrain.universe.Universe;
import com.zillabyte.motherbrain.utils.Utils;

public class LocalDevBufferProducer implements BufferProducer {

  private static final int BUFFER_FLUSH_LIMIT = 100;
  private static final long BUFFER_BYTE_LIMIT = 50_000;
  private SinkToBuffer _operation;
  private List<MapTuple> _buffer = Lists.newLinkedList();
  private long _currentBufferByteSize = 0L;
  private String _authToken;
  private static Logger _log = Utils.getLogger(LocalBufferProducer.class);

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
            
        // Now, flush it...
        Universe.instance().api().appendRelation(
            _operation.getRelation().concreteName(),
            _buffer,
            _operation.getTopFlow().getFlowConfig().getAuthToken()
            );
      }
      
    } catch(APIException e) {
      //Throwables.propagate(e);
      _log.error("api exception: " + e);
    } finally {
      _buffer.clear();
      _currentBufferByteSize = 0L;
    }
  }
  
  

}
