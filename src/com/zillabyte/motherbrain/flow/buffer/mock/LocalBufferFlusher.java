package com.zillabyte.motherbrain.flow.buffer.mock;

import java.util.Collection;

import com.zillabyte.motherbrain.flow.buffer.BufferFlusher;

public class LocalBufferFlusher implements BufferFlusher {

  private Collection<LocalBufferProducer> _producers;

  public LocalBufferFlusher(Collection<LocalBufferProducer> p) {
    _producers = p;
  }

  @Override
  public void flushProducers(String sinkTopic) {
    for(LocalBufferProducer p : _producers) {
      p.flush();
    }
  }

}
