package com.zillabyte.motherbrain.container;


import java.io.File;
import java.io.Serializable;

import com.zillabyte.motherbrain.flow.Flow;


/**
 * Containers concern the execution containers of flows and/or operations. 
 * 
 * @author sjarvie
 *
 */
public interface Container extends Serializable {


  /***
   * Starts the container
   */
  public void start() throws ContainerException;


  /***
   * Builds a command to execute in the container. 
   */
  public ContainerExecuteBuilder buildCommand();


  /***
   * Writes the byte contents to a file inside the container
   */
  public void writeFile(String internalPath, byte[] contents) throws ContainerException;

  /***
   * Reads the contents of an internal file 
   */
  public byte[] readFileAsBytes(String file) throws ContainerException;
  
  
  /***
   * Creates an internal directory
   * @throws ContainerException 
   */
  public void createDirectory(String path) throws ContainerException;
  

  /***
   * shuts down the container
   */
  public void cleanup() throws ContainerException;


  public File getRoot();
  public File getFile(String internalPath) throws ContainerException;
  
  
  public void cacheFlow(Flow flow);
  
  public Flow maybeGetCachedFlow(String id, Integer version);

}
