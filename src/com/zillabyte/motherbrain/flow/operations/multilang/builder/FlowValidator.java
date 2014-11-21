package com.zillabyte.motherbrain.flow.operations.multilang.builder;

import com.zillabyte.motherbrain.flow.App;
import com.zillabyte.motherbrain.flow.Component;
import com.zillabyte.motherbrain.flow.Flow;
import com.zillabyte.motherbrain.flow.components.ComponentInput;
import com.zillabyte.motherbrain.flow.components.ComponentOutput;
import com.zillabyte.motherbrain.flow.operations.Operation;
import com.zillabyte.motherbrain.flow.operations.Sink;
import com.zillabyte.motherbrain.flow.operations.Source;

public class FlowValidator {

    
  public static void validateComponent(Component flow) {
    validateNotEmpty(flow);
    validateTotallySunk(flow);   
  }
  
  public static void validateApp(App flow) {
    validateNotEmpty(flow);
    validateTotallySunk(flow);
  }
  
  
  
  public static void validateNotEmpty(Flow flow) {
    if (flow.getOperations().size() == 0) {
      throw new RuntimeException("The app does not declare any operations.");
    }
  }
  
  public static void validateTotallySunk(Component flow) {
    
    // Init 
    int sources = 0;
    for(Operation op : flow.getOperations()) {
      
      // An unsunk branch is simply an operation that doesn't have any successors, and is not a sink, or a join
      if (op.nextOperations().size() == 0 && op instanceof ComponentOutput == false) {
        throw new RuntimeException("The stream originating from '" + op.namespaceName() + "' does not end in an output.  All streams must end in output.");
      }
      if (op instanceof ComponentInput) {
        sources++;
      }
      if (op instanceof Source) {
        sources++;
      }
    }
    
    if (sources == 0) {
      throw new RuntimeException("The component does not declare any sources.");
    }
      
  }
  
  public static void validateTotallySunk(App flow) {
    
    // Init 
    int sources = 0;
    for(Operation op : flow.getOperations()) {
      // An unsunk branch is simply an operation that doesn't have any successors, and is
      // not a sink, or a join
      if (op.nextOperations().size() == 0 && op instanceof Sink == false) {
        throw new RuntimeException("The stream originating from " + op.namespaceName() + " does not end in a sink. All streams must be sunk.");
      }
      if (op instanceof Source) {
        sources++;
      }
    }
    
    if (sources == 0) {
      throw new RuntimeException("The app does not declare any sources.");
    }
      
    
  }

  public void validate(Flow flow) {
    if (flow instanceof App) {
      validateApp((App) flow);
    } else {
      validateComponent((Component) flow);
    }
  }

}
