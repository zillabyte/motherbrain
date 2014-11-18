package com.zillabyte.motherbrain.coordination;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;

import com.zillabyte.motherbrain.coordination.redis.RedisException;
import com.zillabyte.motherbrain.coordination.redis.TransactionalMessageWrapper;


/****
 * This class exists because we want the CoordinationService interface to expose the `ask...` methods,
 * but we don't want to force all implementations to reinvent the wheel if they dont want to.  That
 * is, we can build the ask-subsystem with the sendMessage(..) and watchForMessage(..) primitives.
 * 
 * Future implementaitons of CoordinationService can simply use the AskWrapper by exposing those
 * primitives and avoid making their own aks(..) implementations, if they don't want to. 
 * 
 * @author jake
 *
 */
public class AskWrapper implements Serializable {

  
  /**
   * 
   */
  private static final long serialVersionUID = -1721540097899692732L;




  /***
   * This interface forces you to delegate handleNewMessage(..) back to AskWrapper, otherwise
   * transactional messages won't work.  
   */
  public static interface AskableService extends CoordinationService {
    public void handleNewMessage(String key, Object message, MessageHandler handler) throws CoordinationException;
  }
  
  
  public static final String ASK_PREFIX = "__ask/";
  private static Logger _log = Logger.getLogger(AskWrapper.class);
  private CoordinationService _service;
  
  
  /***
   * 
   * @param service
   */
  public AskWrapper(CoordinationService service) {
    _service = service;
  }
  
  

  /****
   * This should always be called after the universe is instantiated with this state service.
   *
   * @param stream
   * @param message
   * @param timeout
   * @throws TimeoutException 
   * @throws InterruptedException
   * @throws CoordinationException 
   */
  public final boolean sendTransactionalMessage(ExecutorService exec, final String stream, final Object message, long timeout) throws CoordinationException, TimeoutException {
    
    // Init 
    final UUID returnMessage = UUID.randomUUID();
    final String returnStream = UUID.randomUUID().toString();
    final TransactionalMessageWrapper wrapper = new TransactionalMessageWrapper(message, returnStream, returnMessage);
    final SynchronousQueue<Object> response = new SynchronousQueue<>();
    _log.info("Sending message "+message +" on stream "+stream+". Watching for response on "+returnStream);
    
    // Watch for reply...
    Watcher watcher = _service.watchForMessage(exec, wrapper.returnStream, new MessageHandler() {
      @Override
      public void handleNewMessage(String key, Object raw) throws InterruptedException {
//          _log.info("transactional response on "+returnStream+": " + key + " object: " + raw);
        response.put(raw);
      }
    });
    
    try { 
      // Send the initial question...
      _service.sendMessage(stream, wrapper);
      
      // Wait for the reply...
      debug("waiting for transactional reply for message " + message + " on stream " + stream + " wrapper: " + wrapper);
      Object o = response.poll(timeout, TimeUnit.MILLISECONDS);
      
      
      // Sanity
      if (o == null ) throw new TimeoutException("Timeout waiting for response on stream: " + stream);
      if (o instanceof Exception) throw (RemoteCoordinationException) new RemoteCoordinationException(((Exception)o)).adviseRetry();
      debug("response received "+stream);

    } catch(InterruptedException e) {
      throw (CoordinationException) new CoordinationException(e).adviseRetry();
      
    } finally {
      debug("unsubscribing");
      watcher.unsubscribe();
    }
    
    
    // Done
    return true;
  }
  

  
  

  private void debug(String string) {
    //_log.debug(string);
    //System.err.println(string);
  }



  /***
   * A wrapper around sendMessage(..) that synchronously waits for a response to come back from the listeners.
   * 
   * @param rawStream
   * @param value
   * @param timeout
   * @throws CoordinationException 
   * @throws InterruptedException 
   * @throws CoordinationException 
   */
  public final Object ask(ExecutorService exec, final String rawStream, final Object value, final long timeout) throws TimeoutException, CoordinationException {
    
      // INIT 
//      _log.info("ASK: asking on " + rawStream + " with param " + value + " and timeout: " + timeout);
      final String stream = ASK_PREFIX + rawStream;
      final String responseStream = UUID.randomUUID().toString();
      final SynchronousQueue<Object> response = new SynchronousQueue<>();
      Object reply;
      
      // Init the question
      HashMap<String, Object> map = new HashMap<>();
      map.put("response_stream", responseStream);
      map.put("value", value);
      
      // Watch for response
      final Watcher watcher = _service.watchForMessage(exec, responseStream, new MessageHandler() {
        @Override
        public void handleNewMessage(String key, Object raw) throws InterruptedException {
//          _log.info("Recieved ask response at " + key + ": " + raw);
          response.put(raw);
        }
      });
      
      // Send the message 
      _service.sendMessage(stream, map);
      
      // Get the response, possible timeout
      try {
        reply = response.poll(timeout, TimeUnit.MILLISECONDS);
        if (reply == null) throw new TimeoutException("ask timeout: " + rawStream);
      } catch (InterruptedException e) {
        throw new TimeoutException("interrupted");
      } finally {
        watcher.unsubscribe();
      }
      
      // Errors
      if (reply instanceof Exception) {
        throw (RemoteCoordinationException) new RemoteCoordinationException((Exception)reply).adviseRetry();
      }
      
      // Done
      return reply;
      
  }
  
  
  /***
   * Responds to ASKs
   *
   * @param rawStream
   * @param askHandler
   * @param exec 
   */
  public final Watcher watchForAsk(ExecutorService executor, final String rawStream, final AskHandler askHandler) throws CoordinationException {
    
    // INIT
    final String stream = ASK_PREFIX + rawStream;
//    _log.info("ask watching on: " + stream + " with handler: " + askHandler.toString());
    
    // Start watching...  
    Watcher watcher = _service.watchForMessage(executor, stream, new MessageHandler() {
      
      @Override
      public void handleNewMessage(String key, Object payload) throws Exception {

        // Init 
        Map<?, ?> map = (Map<?, ?>) payload;
        String responseStream = (String) map.get("response_stream");
        Object value = map.get("value");
        
        // Sanity checks
        if (value == null) throw (RedisException) new RedisException("value should not be null!").adviseRetry();
        if (responseStream == null) throw (RedisException) new RedisException("responseStream should not be null!").adviseRetry();
        
        // Call the handler
        final Object result = askHandler.handleAsk(key, value);
        
        // Return sanity
        if (result instanceof Exception) {
          throw new IllegalArgumentException("cannot return type Exception: " + result);
        }
        if (result == null) {
          throw new IllegalArgumentException("cannot return null");
        }
        
        // Send the reply
//        _log.info("ask from: " + stream + " returning to stream: " + responseStream.toString() + " with result: " + result);
        _service.sendMessage(responseStream, result);
        
      }
    
    });

    // DONE
    return watcher;
  }

  
  
  
  /***
   * 
   * @param key
   * @param message
   * @param handler
   * @throws CoordinationException 
   */
  public void handleNewMessage(String key, Object message, MessageHandler handler) throws CoordinationException {
    
    // Init 
    TransactionalMessageWrapper tWrapper = null;
    Object ret = null;
    
    if (message instanceof TransactionalMessageWrapper) {
      
      // Transactional message
//      _log.info("received transactional message: " + key + " msg: " + message);
      tWrapper = (TransactionalMessageWrapper) message;
      message = tWrapper.getMessage();
      ret = tWrapper.getReturnMessage();
      
    } else {
      
      // Normal message
//      _log.info("received normal message: " + key + " msg: " + message);
    }
    
    // Let the handler take care of this...
    try {
      handler.handleNewMessage(key, message);
    } catch(Exception ex) {
      ex.printStackTrace();
      _log.error("message handler threw an error: " + ex.getMessage());
      ret = ex;
    }
    
    // Compelte transaction
    if (tWrapper != null) {
//      _log.info("completing transactional message: " + key);
      _service.sendMessage(tWrapper.getReturnStream(), ret);
    }
  }
  
  
  
}
