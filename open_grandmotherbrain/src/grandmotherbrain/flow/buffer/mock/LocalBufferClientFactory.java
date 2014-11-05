package grandmotherbrain.flow.buffer.mock;

import grandmotherbrain.flow.buffer.BufferClientFactory;
import grandmotherbrain.flow.buffer.BufferConsumer;
import grandmotherbrain.flow.buffer.BufferFlusher;
import grandmotherbrain.flow.buffer.BufferProducer;
import grandmotherbrain.flow.buffer.SinkToBuffer;
import grandmotherbrain.flow.buffer.SourceFromBuffer;

import java.util.List;

import com.google.monitoring.runtime.instrumentation.common.com.google.common.collect.Lists;

public class LocalBufferClientFactory implements BufferClientFactory {

  private List<LocalBufferProducer> _producers = Lists.newLinkedList();
  
  @Override
  public BufferConsumer createConsumer(SourceFromBuffer operation) {
    return new LocalBufferConsumer(operation);
  }

  @Override
  public BufferProducer createProducer(SinkToBuffer operation) {
    LocalBufferProducer l = new LocalBufferProducer(operation);
    _producers.add(l);
    return l;
  }

  @Override
  public BufferFlusher createFlusher() {
    return new LocalBufferFlusher(_producers);
  }

}
