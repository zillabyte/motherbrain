package com.zillabyte.motherbrain.container;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.apache.log4j.Logger;

import com.github.rholder.retry.RetryException;
import com.zillabyte.motherbrain.utils.FileLockUtil;
import com.zillabyte.motherbrain.utils.Utils;
import com.zillabyte.motherbrain.utils.FileLockUtil.MultiLock;

/**
 * Facilitates communication between the container and the host
 * @author sjarvie
 *
 */
public class TcpSocketHelper {

  public static final Logger _log = Utils.getLogger(TcpSocketHelper.class);
  
  public static int getRandomPort() {
    return 10000 + ((int)(Math.random() * 50000));
  }
  
  
  /**
   * Allow the system to allocate a random lock for container to host communication.
   * @return
   */
  public static ServerSocket getNextAvailableTcpSocket() {
    MultiLock lock = null;
    try {
      lock = FileLockUtil.lock("/tmp/port_helper_lock");
      return Utils.retry(3, new Callable<ServerSocket>() {
        @Override
        public ServerSocket call() throws Exception {
          int port = getRandomPort();
          _log.info("attempting to use port: " + port);
          return new ServerSocket(port, 10, InetAddress.getByName("127.0.0.1"));  // passing 0 = let system find next available port
        }
      });
    } catch (IOException | InterruptedException | ExecutionException | RetryException e) { 
      throw new RuntimeException(e);
    } finally {
      try {
        if (lock != null) {
          lock.close();
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  };

}
