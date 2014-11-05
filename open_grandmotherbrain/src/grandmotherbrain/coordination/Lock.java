package grandmotherbrain.coordination;

public interface Lock {

  public void release() throws CoordinationException;
  
}
