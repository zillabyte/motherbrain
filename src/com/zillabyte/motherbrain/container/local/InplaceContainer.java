package com.zillabyte.motherbrain.container.local;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.ServerSocket;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.Logger;
import org.apache.tools.ant.types.Commandline;

import com.google.common.collect.Maps;
import com.zillabyte.motherbrain.container.Container;
import com.zillabyte.motherbrain.container.ContainerException;
import com.zillabyte.motherbrain.container.ContainerExecuteBuilder;
import com.zillabyte.motherbrain.container.TcpSocketHelper;
import com.zillabyte.motherbrain.container.UnixSocketHelper;
import com.zillabyte.motherbrain.container.UnixSocketHelper.AFUNIXServerSocketWrapper;
import com.zillabyte.motherbrain.flow.Flow;
import com.zillabyte.motherbrain.flow.config.FlowConfig;
import com.zillabyte.motherbrain.flow.operations.multilang.MultiLangProcess;
import com.zillabyte.motherbrain.utils.Utils;


/**
 * Local containers write to your local filesystem
 * Allows development on OSX, treat this as the mock implementation
 * @author sjarvie
 *
 */
public class InplaceContainer implements Container, Serializable {

  private static final long serialVersionUID = 1921652389278262665L;
  private boolean _started = false;         
  private File _flowRoot;                               
  private FlowConfig _flowConfig;                         
  private static Logger _log = Logger.getLogger(InplaceContainer.class);
  private Map<String,String> _containerEnv = Maps.newHashMap();

  /**
   * Init local container, uses FlowConfig to match to a flow
   * @param fc
   */
  public InplaceContainer(File flowRoot, FlowConfig fc) {
    _flowConfig = fc;
    // _flowRoot = new File(ContainerPathHelper.externalPathForFlowRoot(_flowConfig.getFlowId(), "flow"));
    _flowRoot = flowRoot;
  }

  @Override
  public void start() throws ContainerException {
    _log.info("starting container " + _flowConfig.getFlowId());
    _started  = true;
  }

  /**
   * Executes a CLI command witin the internal container
   * @param socket
   * @param flowId
   * @param command
   * @return
   * @throws ContainerException
   */
  private MultiLangProcess executeCli(ServerSocket socket, String dir, String... command) throws ContainerException {
    try { 

      // Sanity
      if (!_started) throw new IllegalStateException("start() not called");
      String host = "127.0.0.1";

      // Execute the command in a wrapped /bin/bash, so we get the expected environment. 
      // TODO: remove this wrapper at some point
      StringBuilder sb = new StringBuilder();
      
      // Set env 
      for(Entry<String,String> e : _containerEnv.entrySet()) {
        sb.append(Commandline.quoteArgument(e.getKey()));
        sb.append("=");
        sb.append(Commandline.quoteArgument(e.getValue()));
        sb.append(" ");
      }
      
      // command
      sb.append("zillabyte ");
      for(String s : command) {
        sb.append(Commandline.quoteArgument(s));
        sb.append(" ");
      }

      // sockets
      if (socket != null) {
        if (socket instanceof AFUNIXServerSocketWrapper) {
          // Unix socket... 
          String file = ((AFUNIXServerSocketWrapper)socket).getSocketFile();
          sb.append(" --unix_socket " + Commandline.quoteArgument(file));
        } else {
          // TCP socket.. 
          sb.append(" --host " + host);
          sb.append(" --port " + socket.getLocalPort());
        }
      }
      
      StringBuilder bootstrap = new StringBuilder();
      
      String[] realCommand = {
          "/bin/bash", "-l", "-c",
          bootstrap.toString() + "cd \"" + getFile(dir) + "\"; " +
              sb.toString()};

      // Execute the command
      MultiLangProcess process = MultiLangProcess.create(
          getEnvironment(),
          realCommand,
          socket
          );

      // Done 
      return process;

    } catch(Exception ex) {
      throw new ContainerException(ex);
    }
  }


  /**
   * Create a new base environment for execution
   * @return
   */
  private Map<String, String> getEnvironment() {
    Map<String, String> m = Maps.newHashMap();
    return m;
  }


  /**
   * Get the file at the internalPath within the container filesystem
   * @param internalPath
   * @return
   * @throws ContainerException 
   */
  @Override
  public File getFile(String internalPath) throws ContainerException {
    return new File(FilenameUtils.concat(_flowRoot.getAbsolutePath(), internalPath));
  }
  

  /**
   * For local containers, simply delete the container directory
   */
  @Override
  public void cleanup() throws ContainerException {
    _log.info("cleaning up container " + this._flowConfig.getFlowId() + " " + this._flowRoot);
    _started = false;
    //removeDirectory();
  }
  

  @Override
  public void writeFile(String internalPath, byte[] contents) throws ContainerException {
    throw new IllegalStateException("Files should not be written to InPlace containers");
//    try {
//      File f = new File(_flowRoot, internalPath);
//      Files.write(contents, f);
//    } catch (IOException e) {
//      throw new ContainerException(e);
//    }   
  }
  
  @Override
  public ContainerExecuteBuilder buildCommand() {
    return new ContainerExecuteBuilder() {
      @Override
      public MultiLangProcess createProcess() throws ContainerException {

        // Port? 
        ServerSocket socket = null;
        if (super._withTcpSockets) {
          socket = TcpSocketHelper.getNextAvailableTcpSocket();
        } else if (super._withUnixSockets) {
          // Unix Sockets have a path name limit of 114 chars --> don't use UUIDs 
          String name = Integer.toHexString((int) (Math.random() * 10000));
          File sockFile = getFile("/tmp/" + name + ".sock");
          sockFile.deleteOnExit();
          socket = UnixSocketHelper.createUnixSocket(sockFile);
          // Wait for it to exist...
          while(sockFile.exists() == false) {
            _log.info("waiting for unix socket...");
            Utils.sleep(100);
          }
        }

        // Execute
        return executeCli(socket, _flowRoot.getAbsolutePath(), _command);

      }
    };
  }


  @Override
  public byte[] readFileAsBytes(String file) throws ContainerException {
    try {
      File f = new File(_flowRoot, file);
      return FileUtils.readFileToByteArray(f);
    } catch (IOException e) {
      throw new ContainerException(e);
    }
  }


  @Override
  public void createDirectory(String path) throws ContainerException {
    throw new IllegalStateException("Files should not be written to InPlace containers");
//    File f = new File(_flowRoot, path);
//    f.mkdirs();
  }


  public FlowConfig getFlowConfig() {
    return _flowConfig;
  }
  
  @Override
  public File getRoot() {
    return _flowRoot;
  }
  
  public void setStarted(boolean state) {
    _started = state;
  }
  
  @Override
  public void cacheFlow(Flow flow) {
    // Do nothing, no caching in local mode
    return;
  }
  
  @Override
  public Flow maybeGetCachedFlow(String id, Integer version) {
    // Do nothing, no caching in local mode
    return null;
  }

}
