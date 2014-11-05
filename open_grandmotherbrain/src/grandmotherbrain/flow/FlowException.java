package grandmotherbrain.flow;

import grandmotherbrain.top.MotherbrainException;


/***
 * Use this for errors that originate at the flow level
 * @author jake
 *
 */
public class FlowException extends MotherbrainException {

  private static final long serialVersionUID = -1014775321731054176L;
  

  /***
   * 
   * @param flow
   */
  public FlowException(Flow flow) {
    super();
    _internalMessagePrefix = "[f" + flow.getId() + "]: ";
  }
  
  public FlowException(Flow flow, Throwable ex) {
    super(ex);
    _internalMessagePrefix = "[f" + flow.getId() + "]: ";
  }
  
  public FlowException(String id, Throwable ex) {
    super(ex);
    _internalMessagePrefix = "[f" + id + "]: ";
  }
  
  
}
