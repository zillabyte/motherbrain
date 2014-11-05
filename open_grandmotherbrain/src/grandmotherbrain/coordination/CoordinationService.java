package grandmotherbrain.coordination;

import java.io.Serializable;
import java.util.concurrent.TimeoutException;

public interface CoordinationService extends Serializable {
  
  public void initialize() throws CoordinationException;
  public void shutdown() throws CoordinationException;
  
  
  /***
   * Gets the value associated with this key from the distributed service. 
   * @param key 
   * @param defaultState The default value to return if no state is found 
   * @return
   * @throws CoordinationException
   */
  public <T> T getState(String key, T defaultState) throws CoordinationException;
  
  /***
   * Sets the value associated with this key
   * @param key
   * @param value
   * @throws CoordinationException
   */
  public <T> void setState(String key, T value) throws CoordinationException;
  
  /**
   * Return true if the specified key exists
   * @param key
   * @return
   * @throws CoordinationException
   */
  public boolean hasState(String key) throws CoordinationException;
  
  /**
   * Clears all state beginning with key
   * @param key
   * @throws CoordinationException
   */
  public void removeStateWithPrefix(String key) throws CoordinationException;
  
  /***
   * Clears all state AND locks AND watchers
   * @throws CoordinationException
   */
  public void clear() throws CoordinationException;
  
  
  
  /****
   * Obtains a global lock on a specified key.  Use this when you want to lock a resouce
   * between disparate machines. 
   * 
   * The return value is a Lock object.  You MUST use this object to release the lock later. 
   * You should always wrap the code after lock() in a try {} finally { lock.release() } 
   * so we do not have orphaned locks when errors occur. 
   * 
   * @param lockPath the lock key
   * @param timeout how long to wait before throwing a TimeoutException? 
   * @param duration how long should this lock exist? This is useful when machines die, otherwise locks exist forever 
   * @return
   * @throws CoordinationException
   * @throws TimeoutException 
   */
  public Lock lock(String lockPath, long timeout, long duration) throws CoordinationException, TimeoutException;
  
  
  /***
   * Sends a message to a remote listener.  This method as asynchronous, and does not guarantee delivery
   * or acknowledgment. 
   * 
   * If you require a 'return reciept' -- that is, you wish to assert that the message was recived by 
   * the remote host, then use the sendTransactionalMessage() method
   * 
   * If you require a response from the remoteHost, then use the ask(..) method
   * 
   * @param channel
   * @param message
   * @throws CoordinationException
   */
  public void sendMessage(String channel, Object message) throws CoordinationException;
  
  /***
   * Sends a message with the added gurantee that it has been acknowledged by the remote host. 
   * Kind of like certified mail.
   * 
   * @param channel
   * @param message
   * @param timeout how long to wait before we give up? 
   * @throws CoordinationException 
   * @throws TimeoutException
   */
  public void sendTransactionalMessage(String channel, Object message, long timeout) throws CoordinationException, TimeoutException;
  
  
  /***
   * like he above sendMessage()s, but also gets a response back from the remote agent. 
   * @param channel
   * @param message
   * @param timeout
   * @return
   * @throws CoordinationException
   * @throws TimeoutException
   */
  public Object ask(String channel, Object message, long timeout) throws CoordinationException, TimeoutException;
  
  
  
  /***
   * Watches for messages from remote machines on the cluster.  
   * 
   * @param channel
   * @param messageHandler the callback to handle message
   * @return
   * @throws CoordinationException
   */
  public Watcher watchForMessage(String channel, MessageHandler messageHandler) throws CoordinationException;
  
  
  /***
   * Watches for remote 'asks' from remote machines on the cluster. 
   * 
   * And 'ask' is simply a message that requires a response.  
   * 
   * @param channel
   * @param askHandler
   * @return
   * @throws CoordinationException
   */
  public Watcher watchForAsk(String channel, AskHandler askHandler) throws CoordinationException;
  
}
