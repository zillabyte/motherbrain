package grandmotherbrain.flow.error.strategies;

import grandmotherbrain.flow.operations.Operation;

import java.io.Serializable;

@SuppressWarnings("all")
public abstract class ErrorStrategyFactory implements Serializable {


  public abstract OperationErrorStrategy createOperationStrategy(Operation op);
  
  public abstract FlowErrorStrategy createFlowStrategy();
  
  
  
  public static class Forgiving extends ErrorStrategyFactory {
    
    @Override
    public OperationErrorStrategy createOperationStrategy(Operation op) {
      return new PassiveWorkerPercentageAndAbsoluteOperationErrorStrategy(op);
    }
    
    @Override
    public FlowErrorStrategy createFlowStrategy() {
      return new ForgivingFlowErrorStrategy();
    }
    
  }
  
  
  
  public static class Strict extends ErrorStrategyFactory {

    @Override
    public OperationErrorStrategy createOperationStrategy(Operation op) {
      return new PassiveWorkerPercentageAndAbsoluteOperationErrorStrategy(op);
    }
    
    @Override
    public FlowErrorStrategy createFlowStrategy() {
      return new ForgivingFlowErrorStrategy();
    }
  }
  
  
  
  
}
