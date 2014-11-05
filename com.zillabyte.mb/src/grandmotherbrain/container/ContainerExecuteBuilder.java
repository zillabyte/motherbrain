package grandmotherbrain.container;

import grandmotherbrain.flow.operations.multilang.MultiLangProcess;

import java.util.Map;

import com.google.common.collect.Maps;

/**
 * Creates and configures an execution evironment for commands within a container
 * @author sjarvie
 *
 */
public abstract class ContainerExecuteBuilder {

  protected String _pwd = null;
  protected boolean _withTcpSockets = false;
  protected boolean _withUnixSockets = false;
  protected String[] _command = new String[] {};
  protected Map<String,String> _env = Maps.newHashMap();
  

  /**
   * Sets the PWD
   */
  public ContainerExecuteBuilder inDirectory(String pwd) {
    _pwd = pwd; 
    return this;
  }
  

  /**
   * Sets the PWD based on flow id
   */
  public ContainerExecuteBuilder inFlowDirectory(String flowId) {
    return inDirectory(ContainerPathHelper.internalPathForFlow(flowId));
  }
  
  
  /**
   * Set the container to use sockets
   * @return
   */
  public ContainerExecuteBuilder withTcpSockets() {
    _withTcpSockets = true;
    _withUnixSockets = false;
    return this;
  }
  
  
  /***
   * 
   * @return
   */
  public ContainerExecuteBuilder withUnixSockets() {
    _withUnixSockets = true;
    _withTcpSockets = false;
    return this;
  }
  
  
  /***
   * 
   * @return
   */
  public ContainerExecuteBuilder withSockets() {
    withUnixSockets();
    return this;
  }
  
  
  /**
   * Set the container to not use sockets
   * @return
   */
  public ContainerExecuteBuilder withoutSockets() {
    _withTcpSockets = false;
    return this;
  }

  /**
   * Set the command to run in the container
   * @param command
   * @return
   */
  public ContainerExecuteBuilder withCLICommand(String... command) {

    _command = command;
    return this;
  }

  /**
   * Create the MultilangProcess(if any) associated with command
   * @return
   * @throws ContainerException
   */
  public abstract MultiLangProcess createProcess() throws ContainerException;


  public ContainerExecuteBuilder withEnvironment(Map<String, String> m) {
    _env.putAll(m);
    return this;
  }


  public ContainerExecuteBuilder withEnvironment(String key, String val) {
    _env.put(key, val);
    return this;
  }
  
}
