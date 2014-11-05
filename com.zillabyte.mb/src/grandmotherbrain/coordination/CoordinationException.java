package grandmotherbrain.coordination;

import grandmotherbrain.top.MotherbrainException;

public class CoordinationException extends MotherbrainException {

  private static final long serialVersionUID = 2943295844876339287L;
  
  public CoordinationException() {
    super();
  }
  
  public CoordinationException(Exception ex) {
    super(ex);
  }

  public CoordinationException(String msg) {
    super(msg);
  }

}
