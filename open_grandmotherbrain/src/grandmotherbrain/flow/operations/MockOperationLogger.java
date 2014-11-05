package grandmotherbrain.flow.operations;

import java.util.Date;

import com.google.common.collect.LinkedListMultimap;

public class MockOperationLogger extends OperationLogger.Base {


  private static final long serialVersionUID = -5106446942637587086L;

  private LinkedListMultimap<String, String> _logs = LinkedListMultimap.create();

  private boolean _capture = false;
  
  public MockOperationLogger(String name, String procId) {
    super(name, procId);
  }

  public void clear() {
    _logs.clear();
  }
  
  public boolean contains(String type, String message) {
    for(String m : _logs.get(type)) {
      if (m.contains(message)) {
        return true;
      }
    }
    return false;
  }
  
  
  public void setCapture(boolean b) {
    _capture  = b;
  }
  
  @Override
  public synchronized void writeLogInternal(String message, LogPriority priority) {
    if (message != null) {
      @SuppressWarnings("unused")
      final String internalMessage = toRFC3339(new Date()) + "[" + _procId + "] - [" + priority + "] " + message; 
//      System.err.println(internalMessage);
      if (_capture) {
        _logs.put(priority.toString(), message);
      }
    }
  }


  @Override
  public String absoluteFilename() {
    // TODO Auto-generated method stub
    return null;
  }

}
