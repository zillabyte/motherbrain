package com.zillabyte.motherbrain.coordination;

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
  
  
  
  
  public void initialize() throws CoordinationException {
    _delegate.initialize();
  }

  
  public void shutdown() throws CoordinationException {
    _delegate.shutdown();
  }

  public <T> T getState(String key, T defaultState) throws CoordinationException {
    return _delegate.getState(key, defaultState);
  }
  
  public <T> T getState(String key) throws CoordinationException {
    return _delegate.getState(key, null);
  }

  public <T> void setState(String key, T state) throws CoordinationException {
    _delegate.setState(key, state);
  }

  public boolean hasState(String key) throws CoordinationException {
    return _delegate.hasState(key);
  }

  public void removeStateWithPrefix(String key) throws CoordinationException {
    _delegate.removeStateWithPrefix(key);
  }

  public void clear() throws CoordinationException {
    _delegate.clear();
  }
  
  

  public Lock lock(String lockPath, long timeout, long duration) throws CoordinationException, TimeoutException {
    return _delegate.lock(lockPath, timeout, duration);
  }
  
  public Lock lock(String lockPath, long timeout) throws CoordinationException, TimeoutException {
    return _delegate.lock(lockPath, timeout, getDefaultLockDuration());
  }
  
  public Lock lock(String lockPath) throws CoordinationException, TimeoutException {
    return _delegate.lock(lockPath, getDefaultLockTimeout(), getDefaultLockDuration());
  }
  


  
  
  public void sendMessage(String channel, Object message) throws CoordinationException {
    _delegate.sendMessage(channel, message);
  }

  public void sendTransactionalMessage(String channel, Object message, long timeout) throws CoordinationException, TimeoutException {
    _delegate.sendTransactionalMessage(channel, message, timeout);
  }

  public void sendTransactionalMessage(String channel, Object message) throws CoordinationException, TimeoutException {
    _delegate.sendTransactionalMessage(channel, message, getDefaultTransactionMessageTimeout());
  }
  
  
  public Watcher watchForMessage(String channel, MessageHandler messageHandler) throws CoordinationException {
    return _delegate.watchForMessage(channel, messageHandler);
  }

  public Watcher watchForAsk(String channel, AskHandler askHandler) throws CoordinationException {
    return _delegate.watchForAsk(channel, askHandler);
  }

  public Object ask(String channel, Object message, long timeout) throws CoordinationException, TimeoutException {
    return _delegate.ask(channel, message, timeout);
  }
  
  public Object ask(String channel, Object message) throws CoordinationException, TimeoutException {
    return _delegate.ask(channel, message, getDefaultAskTimeout());
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
