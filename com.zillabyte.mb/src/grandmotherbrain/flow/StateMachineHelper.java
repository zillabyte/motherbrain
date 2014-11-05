package grandmotherbrain.flow;

import grandmotherbrain.utils.Utils;

import org.apache.log4j.Logger;


public class StateMachineHelper {

  private static Logger log = Utils.getLogger(StateMachineHelper.class);
  
  /****
   * 
   * @param _state
   * @param valueOf
   * @return
   * @throws StateMachineException 
   */
  public static <T extends StateMachine<?>> T transition(T oldState, T newState) throws StateMachineException {
    
    if (oldState == null) throw new NullPointerException("old state must not be null");
    if (newState == null) throw new NullPointerException("new state must not be null");
    if (oldState == newState) return oldState;

    if (newState.predecessors().contains(oldState)) {
//      log.info("Transitioning from "+oldState.toString()+" to "+newState.toString());
      return newState;
    } else {
      throw new StateMachineException("cannot transition from " + oldState + " to " + newState);
    }

  }
  
  
  
  
  

}
