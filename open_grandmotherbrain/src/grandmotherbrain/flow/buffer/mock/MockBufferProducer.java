package grandmotherbrain.flow.buffer.mock;

import grandmotherbrain.flow.MapTuple;
import grandmotherbrain.flow.buffer.BufferProducer;
import grandmotherbrain.flow.buffer.SinkToBuffer;
import grandmotherbrain.utils.MeteredLog;

import java.io.Serializable;

import org.apache.log4j.Logger;

import com.google.common.collect.ArrayListMultimap;

public class MockBufferProducer implements BufferProducer, Serializable {

  private static final long serialVersionUID = 5337377114916636077L;
  private SinkToBuffer _operation;
  private String _relationName;
  public static ArrayListMultimap<String, MapTuple> _tuples = ArrayListMultimap.create();
  private static Logger _log = Logger.getLogger(MockBufferProducer.class); 

  
  public MockBufferProducer(SinkToBuffer operation) {
    _operation = operation;
    _relationName = _operation.getTopicName();
  }

  @Override
  public void pushTuple(MapTuple t) {
    synchronized (_tuples) {
      _tuples.put(_relationName, t);
      MeteredLog.info(_log, "size for relation: " + _relationName + " is: " + size(_relationName));
    }
    
  }

  public static int size(String relationName) {
    return _tuples.get(relationName).size();
  }

  public static void reset() {
    synchronized (MockBufferProducer._tuples) {
      _tuples.clear();
    }
  }
  
}
