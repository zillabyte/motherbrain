package grandmotherbrain.flow.buffer;



/**
 * 
 * @author sashi
 *
 */
public interface BufferClientFactory {

  public BufferConsumer createConsumer(SourceFromBuffer operation);
  
  public BufferProducer createProducer(SinkToBuffer operation);

  public BufferFlusher createFlusher();
  
}

