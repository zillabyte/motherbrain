package grandmotherbrain.flow.operations.multilang.builder;

import grandmotherbrain.flow.App;
import grandmotherbrain.flow.Component;
import grandmotherbrain.flow.Flow;
import grandmotherbrain.flow.FlowCompilationException;
import grandmotherbrain.flow.components.ComponentInput;
import grandmotherbrain.flow.components.ComponentOutput;
import grandmotherbrain.flow.operations.Operation;
import grandmotherbrain.flow.operations.Sink;
import grandmotherbrain.flow.operations.Source;

public class FlowValidator {

    
  public static void validateComponent(Component flow) throws FlowCompilationException{
    validateNotEmpty(flow);
    validateTotallySunk(flow);   
  }
  
  public static void validateApp(App flow) throws FlowCompilationException {
    validateNotEmpty(flow);
    validateTotallySunk(flow);
  }
  
  
  
  public static void validateNotEmpty(Flow flow) throws FlowCompilationException {
    if (flow.getOperations().size() == 0) {
      throw new FlowCompilationException("The app does not declare any operations.");
    }
  }
  
  public static void validateTotallySunk(Component flow) throws FlowCompilationException {
    
    // Init 
    int sources = 0;
    for(Operation op : flow.getOperations()) {
      
      // An unsunk branch is simply an operation that doesn't have any successors, and is not a sink, or a join
      if (op.nextOperations().size() == 0 && op instanceof ComponentOutput == false) {
        throw new FlowCompilationException("The stream originating from '" + op.namespaceName() + "' does not end in an output.  All streams must end in output.");
      }
      if (op instanceof ComponentInput) {
        sources++;
      }
      if (op instanceof Source) {
        sources++;
      }
    }
    
    if (sources == 0) {
      throw new FlowCompilationException("The component does not declare any sources.");
    }
      
  }
  
  public static void validateTotallySunk(App flow) throws FlowCompilationException {
    
    // Init 
    int sources = 0;
    for(Operation op : flow.getOperations()) {
      // An unsunk branch is simply an operation that doesn't have any successors, and is
      // not a sink, or a join
      if (op.nextOperations().size() == 0 && op instanceof Sink == false) {
        throw new FlowCompilationException("The stream originating from " + op.namespaceName() + " does not end in a sink.  All streams must be sunk.");
      }
      if (op instanceof Source) {
        sources++;
      }
    }
    
    if (sources == 0) {
      throw new FlowCompilationException("The app does not declare any sources.");
    }
      
    
  }

  public void validate(Flow flow) throws FlowCompilationException {
    if (flow instanceof App) {
      validateApp((App) flow);
    } else {
      validateComponent((Component) flow);
    }
  }

}
