package com.zillabyte.motherbrain.flow.operations;

import com.zillabyte.motherbrain.top.MotherbrainException;


/***
 * Use this for errors that originate at the flow level
 * @author jake
 *
 */
public class LoopException extends MotherbrainException {

  private static final long serialVersionUID = -1014775321731054176L;
  
  void setOperation(Operation op) {
    this._messagePrefix = "[f" + op.topFlowId() + "-" + op.instanceName() + "]: ";
  }
  
  // For mock only
  public LoopException(String s) {
    super(s);
  }
  
  public LoopException(Operation op, Throwable ex) {
    super(ex.getMessage(), ex);
    setOperation(op);
  }

  public LoopException(Operation op, String string) {
    super(string);
    setOperation(op);
  }
  
  public LoopException(Operation op, String string, Throwable ex) {
    super(string, ex);
    setOperation(op);
  }
  
  public static class MockOperationException extends LoopException {
    /**
     * 
     */
    private static final long serialVersionUID = 4982126439104819116L;

    public MockOperationException(String s) {
      super(s);
    }
  }
  
}
