package grandmotherbrain.flow.collectors.coordinated;

public class DeadNodeDetectedException extends RuntimeException {

  private static final long serialVersionUID = -8161480839110060715L;
  
  public DeadNodeDetectedException(String s) {
    super(s);
  }

}
