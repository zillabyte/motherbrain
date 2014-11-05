package grandmotherbrain.coordination;


public interface Watcher {

  public void unsubscribe() throws CoordinationException;
  
}
