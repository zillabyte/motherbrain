package grandmotherbrain.flow.buffer.mock;

import grandmotherbrain.flow.buffer.BufferFlusher;

import java.util.Collection;

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
