package com.zillabyte.motherbrain.container;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.nio.file.Files;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.newsclub.net.unix.AFUNIXServerSocket;
import org.newsclub.net.unix.AFUNIXSocketAddress;

import com.google.monitoring.runtime.instrumentation.common.com.google.common.base.Throwables;
import com.zillabyte.motherbrain.utils.FileLockUtil;
import com.zillabyte.motherbrain.utils.FileLockUtil.MultiLock;
import com.zillabyte.motherbrain.utils.Utils;


public class UnixSocketHelper {

  
  private static Logger _log = Utils.getLogger(UnixSocketHelper.class);
  
  /*****
   * We override AFUNIXServerSocket here because we want to expose the `getSocketFile` method 
   */
  public static class AFUNIXServerSocketWrapper extends AFUNIXServerSocket {

    private AFUNIXSocketAddress _endpoint;

    protected AFUNIXServerSocketWrapper() throws IOException {
      super();
    }
    
    public void bind(SocketAddress endpoint, int backlog) throws IOException {
      super.bind(endpoint, backlog);
      _endpoint = (AFUNIXSocketAddress) endpoint;
    }
    
    public String getSocketFile() {
      return _endpoint.getSocketFile();
    }
    
  }
  
  
  
  /**
   * Synchronized doesn't help in a multi-process env.
   * @throws Exception
   */
  public synchronized static void maybeUnpackNativeLibraries() throws Exception {
    
    File destDir = new File("/tmp/junixsocket");
    File destDirReady = new File("/tmp/junixsocket/ready");
    if(destDirReady.exists()){
      // If the ready file exists, set path and move on.
      _log.info("socket dir was already ready. Setting unix library path: " + destDir.getAbsolutePath());
      System.setProperty("org.newsclub.net.unix.library.path", destDir.getAbsolutePath());
      return; 
    }
    
    MultiLock lock = FileLockUtil.lock("/tmp/junixsocket.lock");
    try { 
    
      if(destDirReady.exists()){
        // Try again, just in case of race conditions...
        _log.info("socket dir was already ready. Setting unix library path: " + destDir.getAbsolutePath());
        System.setProperty("org.newsclub.net.unix.library.path", destDir.getAbsolutePath());
        return; 
      }
      
      try {
        FileUtils.deleteQuietly(destDir);
        destDir.mkdirs();
        InputStream stream = UnixSocketHelper.class.getResourceAsStream("/lib-native/junixsocket.zip");
        File zip = new File(destDir, "junixsocket.zip");
        Files.copy(stream, zip.toPath());
        Utils.bash("cd " + destDir.getAbsolutePath() + "; unzip " + zip.getAbsolutePath());
        // Now remove the lock.
        destDirReady.createNewFile(); // Release the lock
      } catch(Exception e) {
        FileUtils.deleteQuietly(destDir);
        Throwables.propagate(e);
      }
      
      _log.info("Setting unix library path: " + destDir.getAbsolutePath());
      System.setProperty("org.newsclub.net.unix.library.path", destDir.getAbsolutePath());
    } finally {
      lock.release();
    }
  }
  
  
  public static ServerSocket createUnixSocket(File socketFile) throws ContainerException {
    try {
      
      maybeUnpackNativeLibraries();
      AFUNIXServerSocket server = new AFUNIXServerSocketWrapper();
      server.bind(new AFUNIXSocketAddress(socketFile));
      return server;
    } catch (Exception e) {
      throw (ContainerException) new ContainerException(e).setUserMessage("Failed to initialize Unix Socket.").adviseRetry();
    }
  }
  
}
