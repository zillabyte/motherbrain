package com.zillabyte.motherbrain.top;


/***
 * Base class for all errors originating out of GMB.  This has some helper methods 
 * on it to better craft user-facing messages, as well as hints as to how to recover
 * from the error (if at all)
 * @author jake
 *
 */
public abstract class MotherbrainException extends Exception {

  
  private static final long serialVersionUID = -5435531522427865286L;
  
  // message for zb engineers
  private String _message;        
  
  protected String _messagePrefix = "";
  
  private final long _date;
  
  ///////////////////////////////////////////////////////////////////////////////

  protected MotherbrainException(final String message, final Throwable cause) {
    super(message, cause);
    _message = message;
    _date = System.currentTimeMillis();
  }
  
  public MotherbrainException(String message) {
    this(message, null);
  }

  public MotherbrainException(Throwable e) {
    this(e.getMessage(), e);
  }
  
  ////////////////////////////////////////////////////////////////////

  @Override
  public String getMessage() {
    if (_message != null) {
      return _messagePrefix + _message;
    } else {
      return _messagePrefix + super.getMessage();
    }
  }

  public MotherbrainException setMessage(String message) {
    this._message = message;
    return this;
  }

  public long getDate() {
    return this._date;
  }
  
  public MotherbrainException adviseRetry() {
    _message += " If there are no other errors, please try re-pushing. If the problem persists, please contact support@zillabyte.com. We apologize for the inconvenience.";
    return this;
  }
  
  
//  @Override
//  public String toString() {
//    StringBuilder sb = new StringBuilder();
////    sb.append(this.getClass().getSimpleName() + "\n");
////    if (this._userMessage != null) {
////      sb.append("user message: " );
////      sb.append(this._userMessage);
////      sb.append("\n");
////    }
////    if (this._internalMessage != null) {
////      sb.append("internal message: " );
////      sb.append(this._internalMessage);
////      sb.append("\n");
////    }
////    sb.append("stack trace: ");
////    for(StackTraceElement t : this.getStackTrace()) {
////      sb.append("  ");
////      sb.append(t.toString());
////      sb.append("\n");
////    }
//    return sb.toString();
//  }
    
  
  public static MotherbrainException getRoot(Throwable t) {
    if (t.getCause() != null) {
      MotherbrainException tt = getRoot(t.getCause());
      if (tt != null) return tt;
    }
    if (t instanceof MotherbrainException) {
      return (MotherbrainException) t;
    } else {
      return null;
    }
  }
  

}
