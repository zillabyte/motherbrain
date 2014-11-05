package com.zillabyte.motherbrain.container;

import java.io.Serializable;

import com.zillabyte.motherbrain.flow.Flow;

/****
 * This iterface exists because of a subtlety in how containers are CREATED and then USED.
 * When a container is created, it is created at the FLOW level.  We only run --prep and
 * --info at that stage, so this isn't a problem.  But when we send to the workers, we can
 * possibly have multiple containers of the same flow on the same machine.  This can create
 * a problem. 
 * 
 * So, instead, when we deserialize a container, we deserialize it to a /flows/fID-instName/ 
 * directory.  We do this because we start a container per single operation instance, and guarantee
 * no overlaps. 
 * 
 * @author jake
 *
 */
public interface ContainerSerializer extends Serializable {

  /**
   * Serialize a container for distribution, after being created at the FLOW level
   * @param flow
   * @throws ContainerException
   */
  public void serializeFlow(Flow flow) throws ContainerException;


  /**
   * Deserialize a container at the INSTANCE level, placing it into /flows/fID-instName/
   * @param con
   * @param instName
   * @throws ContainerException
   */
  public void deserializeOperationInstance(Container con, String instName) throws ContainerException;



}
