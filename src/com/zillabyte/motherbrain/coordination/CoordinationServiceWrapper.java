package com.zillabyte.motherbrain.coordination;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;

import com.zillabyte.motherbrain.universe.Config;


/***
 * Convenience class to provide overloaded methods for the interface
 * @author jake
 *
 */
public class CoordinationServiceWrapper implements CoordinationService {

  
  /**
   * 
   */
  private static final long serialVersionUID = -6365479966023733518L;
  private CoordinationService _delegate;
  

  /***
   * 
   * @param delegate
   */
  public CoordinationServiceWrapper(CoordinationService delegate) {
    _delegate = delegate;
  }
  
  
  
  
  public void initialize() {
    _delegate.initialize();
  }

  
  public void shutdown() {
    _delegate.shutdown();
  }

  public <T> T getState(String key, T defaultState) {
    return _delegate.getState(key, defaultState);
  }
  
  public <T> T getState(String key) {
    return _delegate.getState(key, null);
  }

  public <T> void setState(String key, T state) {
    _delegate.setState(key, state);
  }

  public boolean hasState(String key) {
    return _delegate.hasState(key);
  }

  public void removeStateWithPrefix(String key) {
    _delegate.removeStateWithPrefix(key);
  }

  public void clear() {
    _delegate.clear();
  }
  
  

  public Lock lock(String lockPath, long timeout, long duration) {
    return _delegate.lock(lockPath, timeout, duration);
  }
  
  public Lock lock(String lockPath, long timeout) {
    return _delegate.lock(lockPath, timeout, getDefaultLockDuration());
  }
  
  public Lock lock(String lockPath) {
    return _delegate.lock(lockPath, getDefaultLockTimeout(), getDefaultLockDuration());
  }
  


  
  
  public void sendMessage(String channel, Object message) {
    _delegate.sendMessage(channel, message);
  }

  public void sendTransactionalMessage(ExecutorService exec, String channel, Object message, long timeout) {
    _delegate.sendTransactionalMessage(exec, channel, message, timeout);
  }

  public void sendTransactionalMessage(ExecutorService exec, String channel, Object message) throws TimeoutException {
    _delegate.sendTransactionalMessage(exec, channel, message, getDefaultTransactionMessageTimeout());
  }
  
  
  public Watcher watchForMessage(ExecutorService exec, String channel, MessageHandler messageHandler) {
    return _delegate.watchForMessage(exec, channel, messageHandler);
  }

  public Watcher watchForAsk(ExecutorService exec, String channel, AskHandler askHandler) {
    return _delegate.watchForAsk(exec, channel, askHandler);
  }

  public Object ask(ExecutorService exec, String channel, Object message, long timeout) {
    return _delegate.ask(exec, channel, message, timeout);
  }
  
  public Object ask(ExecutorService exec, String channel, Object message) throws TimeoutException {
    return _delegate.ask(exec, channel, message, getDefaultAskTimeout());
  }

  
  

  private long getDefaultLockTimeout() {
    return Config.getOrDefault("coordination.service.default.lock.timeout.ms", 1000L * 60 * 10);
  }
  
  private long getDefaultLockDuration() {
    return Config.getOrDefault("coordination.service.default.lock.duration.ms", 1000L * 60 * 3);
  }
  
  private long getDefaultTransactionMessageTimeout() {
    return Config.getOrDefault("coordination.service.default.transaction.message.timeout.ms", 1000L * 60 * 3);
  }
  
  private long getDefaultAskTimeout() {
    return Config.getOrDefault("coordination.service.default.ask.timeout.ms", 1000L * 60 * 1);
  }
  
  
  
  
  
  
  
}
