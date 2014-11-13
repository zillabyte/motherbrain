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
  private String _internalMessage;        
  
  // to show the user (optional)
  private String _userMessage;
  
  protected String _internalMessagePrefix = "";
  
  private final long _date;
  
  ///////////////////////////////////////////////////////////////////////////////

  private MotherbrainException(final String internalMessage, final String userMessage, final Throwable cause) {
    super(cause);
    _date = System.currentTimeMillis();
    this._internalMessage = internalMessage;
    this._userMessage = userMessage;
  }
  
//  public MotherbrainException(String internalMessage, String userMessage, Throwable cause) {
//    this(null, internalMessage, userMessage, cause);
//  }

  public MotherbrainException(String internalMessage, String userMessage) {
    this(internalMessage, userMessage, null);
  }
  
  public MotherbrainException(String internalMessage, Throwable cause) {
    this(internalMessage, null, cause);
  }
  
  public MotherbrainException(String internalMessage) {
    this(internalMessage, null, null);
  }

  public MotherbrainException() {
    this(null, null, null);
  }

  public MotherbrainException(Throwable e) {
    this(null, null, e);
  }
  
  ////////////////////////////////////////////////////////////////////

  @Override
  public String getMessage() {
    if (_internalMessage != null) {
      return _internalMessagePrefix + _internalMessage;
    } else if (_userMessage != null) {
      return _internalMessagePrefix + _userMessage;
    } else {
      return _internalMessagePrefix + super.getMessage();
    }
  }

  public String getInternalMessage() {
    return _internalMessage;
  }

  public MotherbrainException setInternalMessage(String _internalMessage) {
    this._internalMessage = _internalMessage;
    return this;
  }

  public String getUserMessage() {
    return _userMessage;
  }

  public MotherbrainException setUserMessage(String _userMessage) {
    this._userMessage = _userMessage;
    if (this._internalMessage == null) 
      _internalMessage = _userMessage;
    return this;
  }


  public long getDate() {
    return this._date;
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
  
  
  
  public static String getRootUserMessage(Throwable t, String def) {
    MotherbrainException tt = getRoot(t);
    if (tt != null && tt.getUserMessage() != null && !tt.getUserMessage().equals("null")) {
      return tt.getUserMessage();
    } else {
      return def;
    }
  }
  

}
