package grandmotherbrain.container;


/**
 * Sets up a communication gateway being external machine and the internal container
 * @author sjarvie
 *
 */
public interface PipeGateway {

  public abstract String getExternalHost();

  public abstract Integer getExternalPort();

  public abstract void cleanup();

}