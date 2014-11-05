package grandmotherbrain.container;

import grandmotherbrain.top.MotherbrainException;

/**
 * Exceptions related to the container interface itself
 * @author sjarvie
 *
 */
public class ContainerException extends MotherbrainException {


  private static final long serialVersionUID = -789788594951874062L;

  public ContainerException(String s) {
    super(s);
  }

  public ContainerException(Throwable e) {
    super(e);
  }

}
