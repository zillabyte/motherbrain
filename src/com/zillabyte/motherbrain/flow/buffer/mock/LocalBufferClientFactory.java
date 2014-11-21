package com.zillabyte.motherbrain.flow.buffer.mock;

import com.zillabyte.motherbrain.flow.buffer.BufferClientFactory;
import com.zillabyte.motherbrain.flow.buffer.BufferConsumer;
import com.zillabyte.motherbrain.flow.buffer.BufferFlusher;
import com.zillabyte.motherbrain.flow.buffer.BufferProducer;
import com.zillabyte.motherbrain.flow.buffer.SinkToBuffer;
import com.zillabyte.motherbrain.flow.buffer.SourceFromBuffer;

public class LocalBufferClientFactory implements BufferClientFactory {

  @Override
  public BufferConsumer createConsumer(SourceFromBuffer operation) {
    return new LocalBufferConsumer(operation);
  }

  @Override
  public BufferProducer createProducer(SinkToBuffer operation) {
    return new LocalBufferProducer(operation);
  }

  @Override
  public BufferFlusher createFlusher() {
    return null;
  }

}
