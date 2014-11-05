package grandmotherbrain.flow.buffer;

import grandmotherbrain.flow.MapTuple;
import net.sf.json.JSONObject;

/**
 * 
 * @author sashi
 *
 */
public interface BufferConsumer {

  public MapTuple getNextTuple();
  public boolean isEmitComplete();
  
  public JSONObject createSnapshot();
  public void applySnapshot(JSONObject snapshot);
  
} 
