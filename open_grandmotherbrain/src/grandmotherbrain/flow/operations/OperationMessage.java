package grandmotherbrain.flow.operations;

import java.io.Serializable;

import org.eclipse.jdt.annotation.NonNullByDefault;

@NonNullByDefault
public final class OperationMessage implements Serializable {
  
  private static final long serialVersionUID = 7458911694450468873L;
  final String command;
  final Object msg;
  final String operationName;
  final String instanceId;

  public OperationMessage(final String operation, final String instanceId, final String command, final Object msg) {
    this.operationName = operation;
    this.instanceId = instanceId;
    this.command = command;
    this.msg = msg;
  }
  
  public String getCommand() {
    return command;
  }
  
  public Object getMessage() {
    return msg;
  }
  
  public String getOperationName() {
    return operationName;
  }
  
  public String getInstanceName() {
    return instanceId;
  }

  public static OperationMessage create(Operation operation, String command, Object msg) {
    return new OperationMessage(operation.namespaceName(), operation.instanceName(), command, msg);
  }
  
  @Override
  public String toString() {
    return "operationMessage: " + getCommand() + " : " + msg.toString();
  }
  
}
