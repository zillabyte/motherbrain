package com.zillabyte.motherbrain.flow.buffer.mock;

import java.util.List;

import com.google.monitoring.runtime.instrumentation.common.com.google.common.collect.Lists;
import com.zillabyte.motherbrain.flow.buffer.BufferClientFactory;
import com.zillabyte.motherbrain.flow.buffer.BufferConsumer;
import com.zillabyte.motherbrain.flow.buffer.BufferFlusher;
import com.zillabyte.motherbrain.flow.buffer.BufferProducer;
import com.zillabyte.motherbrain.flow.buffer.SinkToBuffer;
import com.zillabyte.motherbrain.flow.buffer.SourceFromBuffer;

public class LocalDevBufferClientFactory implements BufferClientFactory {

  private List<LocalDevBufferProducer> _producers = Lists.newLinkedList();
  
  @Override
  public BufferConsumer createConsumer(SourceFromBuffer operation) {
    return new LocalBufferConsumer(operation);
  }

  @Override
  public BufferProducer createProducer(SinkToBuffer operation) {
    LocalDevBufferProducer l = new LocalDevBufferProducer(operation);
    _producers.add(l);
    return l;
  }

  @Override
  public BufferFlusher createFlusher() {
    return new LocalDevBufferFlusher(_producers);
  }

}
