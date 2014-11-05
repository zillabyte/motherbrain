package grandmotherbrain.coordination.redis;

import java.io.Serializable;

public class TransactionalMessageWrapper implements Serializable {

  /**
   * 
   */
  private static final long serialVersionUID = -3850547961951095683L;
  public final Object message;
  public final String returnStream;
  public final Object returnMessage;
  
  public TransactionalMessageWrapper(
      final Object message,
      final String returnStream,
      final Object returnMessage) {
    this.message = message;
    this.returnStream = returnStream;
    this.returnMessage = returnMessage;
  }

  @Override
  public final String toString() {
    return "TransactionalMessageWrapper<message="
         + message + " returnStream="
         + returnStream + " returnMessage="
         + returnMessage + ">";
  }

  public Object getMessage() {
    return message;
  }
  
  public Object getReturnMessage() {
    return returnMessage;
  }
  
  public String getReturnStream() {
    return returnStream;
  }
}