package grandmotherbrain.flow.buffer;

import grandmotherbrain.flow.MapTuple;

/**
 * 
 * @author sashi
 *
 */
public interface BufferProducer {
  public void pushTuple(MapTuple t);
}
