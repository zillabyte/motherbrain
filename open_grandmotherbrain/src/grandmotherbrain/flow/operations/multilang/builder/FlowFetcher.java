package grandmotherbrain.flow.operations.multilang.builder;

import grandmotherbrain.container.ContainerException;
import grandmotherbrain.flow.Flow;
import grandmotherbrain.flow.FlowCompilationException;

import java.io.Serializable;

import net.sf.json.JSONObject;



/**
 * Fetches flows.  
 * @author jake
 *
 */
public interface FlowFetcher extends Serializable {
  
  public Flow buildFlow(String flowId, JSONObject config) throws FlowCompilationException, ContainerException;
  public Flow buildFlow(String flowId) throws FlowCompilationException, ContainerException;
  
}
