package grandmotherbrain.top;

import org.eclipse.jdt.annotation.NonNullByDefault;

@NonNullByDefault
public class MotherbrainRuntimeException extends RuntimeException {

  /**
   * 
   */
  private static final long serialVersionUID = -3578178481683501601L;
  
  final MotherbrainException cause;

  /* This should be the ONLY way we ever use MotherbrainRuntimeException.
   * It should only be used when we are interacting with a non-Zillabyte
   * API that does not allow us to raise a more specialized exception.
   * Ideally, this should also be the only class inheriting from
   * RuntimeException that we deliberately raise.
   */
  public MotherbrainRuntimeException(final MotherbrainException ex) {
    super(ex);
    this.cause = ex;
  }
  
  @Override
  public synchronized MotherbrainException getCause()
  {
    return cause;
  }

}
