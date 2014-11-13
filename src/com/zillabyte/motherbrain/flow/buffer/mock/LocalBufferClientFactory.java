package com.zillabyte.motherbrain.flow.buffer.mock;

import com.zillabyte.motherbrain.flow.buffer.BufferClientFactory;
import com.zillabyte.motherbrain.flow.buffer.BufferConsumer;
import com.zillabyte.motherbrain.flow.buffer.BufferFlusher;
import com.zillabyte.motherbrain.flow.buffer.BufferProducer;
import com.zillabyte.motherbrain.flow.buffer.SinkToBuffer;
import com.zillabyte.motherbrain.flow.buffer.SourceFromBuffer;
import com.zillabyte.motherbrain.flow.operations.OperationException;

public class LocalBufferClientFactory implements BufferClientFactory {

  @Override
  public BufferConsumer createConsumer(SourceFromBuffer operation) throws OperationException {
    return new LocalBufferConsumer(operation);
  }

  @Override
  public BufferProducer createProducer(SinkToBuffer operation) throws OperationException {
    return new LocalBufferProducer(operation);
  }

  @Override
  public BufferFlusher createFlusher() {
    return null;
  }

}
