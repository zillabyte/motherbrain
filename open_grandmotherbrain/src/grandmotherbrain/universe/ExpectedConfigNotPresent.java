package grandmotherbrain.universe;

public class ExpectedConfigNotPresent extends RuntimeException {
  /**
   * There is absolutely no way to responsibly handle this, and it is
   * essentially impossible to recover from.
   */
  private static final long serialVersionUID = 6759802386065625467L;

  public ExpectedConfigNotPresent() {
    super();
  }
}
