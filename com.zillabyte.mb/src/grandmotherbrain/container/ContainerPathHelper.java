package grandmotherbrain.container;

import grandmotherbrain.universe.Universe;

/**
 * Generates:
 *     - internal(within the view of the container itself) 
 *     - external(the machine filesystem)
 * paths for various locations related to containers
 * 
 * @author sjarvie
 *
 */
public class ContainerPathHelper {

  public static String internalPathForFlow(String flowId) {
    return "/app/flows/" + flowId;
  }


  public static String internalPathForPipe(String flowId, String name) {
    return "/pipes/" + flowId + "/" + name;
  }

  
  /**
   * The absolute(external) path of a container
   * @param flowId
   * @param instName
   * @return
   */
  public static String externalPathForFlowRoot(String flowId, String instName) {
    return Universe.instance().fileFactory().getFlowRoot(flowId, instName).getAbsolutePath();
  }

}
