package grandmotherbrain.coordination;


public interface AskHandler {

  public Object handleAsk(String key, Object payload) throws Exception;  
  
}
