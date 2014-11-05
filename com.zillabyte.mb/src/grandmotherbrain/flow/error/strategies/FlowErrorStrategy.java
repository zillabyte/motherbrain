package grandmotherbrain.flow.error.strategies;

import grandmotherbrain.flow.FlowInstanceSetBuilder;

import org.eclipse.jdt.annotation.NonNullByDefault;

/****
 * The vision for this class is to swap it out with better error handling strategies 
 * later on.  Currently, we flow-error if all instances of an operation are in ERROR. 
 * This may prove to be insufficient.  Other ideas: only allow X errors in a given
 * window; only allow X% errors of all tuples; etc. 
 * 
 * The general idea: be forgiving of one-off/gamma-ray errors; but stop the flow if 
 * there's a system error
 * @author jake
 *
 */
@NonNullByDefault
public interface FlowErrorStrategy {

  boolean shouldTransitionToFlowError(FlowInstanceSetBuilder setBuilder);
  
}
