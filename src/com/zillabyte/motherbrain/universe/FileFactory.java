package com.zillabyte.motherbrain.universe;

import java.io.File;
import java.io.Serializable;

/* 
 * IS THERE ALSO A flowRoot?? (from FlowHelper.java, the dir in getWithBinaryTo -- this appears to be just a directory created using createTempDir in MultiLangFlowFetcherAPI)
 * Also, ARE THERE ALSO SINK DIRs?? (from RedshiftStreamWriteHandler, look at ensureFile)
 */

public abstract class FileFactory implements Serializable{

  /**
   * 
   */
  private static final long serialVersionUID = -8269141626826337826L;

  // Set up dirs for operations and flows.
  public abstract void init();
  
  // Where tmp dir is located.
  public abstract File getTmp();

  // Set roots for operations and flows...
  public abstract File getFlowLoggingRoot(String flowId);
  public abstract File getFlowRoot(String flowId, String instName);
  public abstract File getFlowQueueRoot(String flowId, String instName);

  public abstract File flowRoot();
  public abstract File logRoot();
  
  
  
  /****
   * A common base for FileFactories that share a common root
   * @author jake
   *
   */
  public static abstract class RootedFileFactory extends FileFactory {

    private static final long serialVersionUID = 3800863428120462196L;
    
    protected File _flowRoot;
    protected File _logRoot;

    public RootedFileFactory(File flowRoot, File logRoot) {
      _flowRoot = flowRoot;
      _logRoot = logRoot;
    }

    @Override
    public File flowRoot() {
      return _flowRoot;
    }
    
    @Override
    public File logRoot() {
      return _logRoot;
    }
    
    @Override
    public void init() {
      _flowRoot.mkdirs();
    }
    
    protected File ensure(File file) {
      file.mkdirs();
      return file;
    }

    @Override
    public File getFlowRoot(String flowId, String instName) {
      return ensure(new File(_flowRoot, "f" + flowId + "-" + instName));
    }
    

    @Override
    public File getFlowQueueRoot(String flowId, String instName) {
      return ensure(new File(getFlowRoot(flowId, instName), "queue"));
    }
    

    @Override
    public File getTmp() {
      return new File("/tmp");
    }
  }
  

  
  /**
   * 
   *
   */
  public static class AWS extends RootedFileFactory {

    private static final long serialVersionUID = -9178195550089451386L;

    public AWS() {
      super(new File("/mnt/flows"), new File("mnt/logplex"));
    }
    
    @Override
    public File getFlowLoggingRoot(String flow) {
      return ensure(new File(_logRoot, "flow_"+ flow));
    }

  }
  
  
  /**
   * 
   *
   */
  public static class Vagrant extends RootedFileFactory {

    private static final long serialVersionUID = -6420757740122560374L;

    public Vagrant() {
      super(new File("/mnt/flows"), new File("/mnt/logplex"));
    }
    
    @Override
    public File getFlowLoggingRoot(String flow) {
      return ensure(new File(_logRoot, "flow_"+ flow));
    }
    
  }
  

  /**
   * 
   *
   */
  public static class Local extends RootedFileFactory {

    private static final long serialVersionUID = -8330343901777269849L;

    public Local() {
      super(new File("/tmp/flows"), new File("/tmp/flows"));
    }
    
    @Override
    public File getFlowLoggingRoot(String flow) {
      return ensure(new File(_logRoot, "f" + flow + "/flow_logs"));
    }
    
  }
  
  
  
  /**
   * 
   *
   */
  public static class Mock extends RootedFileFactory {

    private static final long serialVersionUID = -6996178412167232546L;

    public Mock() {
      super(new File("/tmp/flows_mock/"), new File("/tmp/flows_mock/"));
    }
    
    @Override
    public File getFlowLoggingRoot(String flow) {
      return ensure(new File(_logRoot, "f" + flow + "/flow_logs"));
    }
    
  }


  
  
  
}
