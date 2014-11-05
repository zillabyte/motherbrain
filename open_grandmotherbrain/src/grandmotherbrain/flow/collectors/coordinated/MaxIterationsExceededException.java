package grandmotherbrain.flow.collectors.coordinated;

public class MaxIterationsExceededException extends RuntimeException {


  private static final long serialVersionUID = 3215975692168353056L;

  
  public MaxIterationsExceededException(Throwable e) {
    super(e);
  }
  
  public MaxIterationsExceededException(String e) {
    super(e);
  }

}
