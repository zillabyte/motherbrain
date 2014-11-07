package com.zillabyte.motherbrain.flow.buffer.mock;

import java.util.Collection;

import com.zillabyte.motherbrain.flow.buffer.BufferFlusher;

public class LocalDevBufferFlusher implements BufferFlusher {

  /**
   * 
   */
  private static final long serialVersionUID = 6986559991916766720L;
  
  private Collection<LocalDevBufferProducer> _producers;

  public LocalDevBufferFlusher(Collection<LocalDevBufferProducer> p) {
    _producers = p;
  }

  @Override
  public void flushProducers(String sinkTopic) {
    for(LocalDevBufferProducer p : _producers) {
      p.flush();
    }
  }

}
