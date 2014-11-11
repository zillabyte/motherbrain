package com.zillabyte.motherbrain.coordination.mock;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.javatuples.Pair;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.zillabyte.motherbrain.coordination.AskHandler;
import com.zillabyte.motherbrain.coordination.AskWrapper;
import com.zillabyte.motherbrain.coordination.CoordinationException;
import com.zillabyte.motherbrain.coordination.CoordinationService;
import com.zillabyte.motherbrain.coordination.MessageHandler;
import com.zillabyte.motherbrain.coordination.Watcher;
import com.zillabyte.motherbrain.utils.Utils;

public class MockStateService implements CoordinationService, AskWrapper.AskableService {

  
  /**
   * 
   */
  private static final long serialVersionUID = -862735335179265034L;

  private static Logger _log = Logger.getLogger(MockStateService.class);
  
  public Map<String, byte[]> _state = Maps.newConcurrentMap();  // public for test inspection
  public Map<String, Lock> _locks = Maps.newConcurrentMap();  
  public List<Pair<String, MessageHandler>> _messageWatchers = Lists.newArrayList();
  public Integer _defer = 0;
  private AskWrapper _askWrapper = new AskWrapper(this);
  
  
  
  

  public class MessageWatcher implements Watcher {
    private Pair<String, MessageHandler> _p;
    public MessageWatcher(Pair<String, MessageHandler> p) {
      _p = p;
    }
    @Override
    public void unsubscribe() {
      synchronized(_messageWatchers) {
        _messageWatchers.remove(_p);
      }
    }
  }
  
  
  
  public MockStateService() {
  }
  
  public MockStateService(Map<String, Object> config) {
    this();
    synchronized (_state) {
      for(Entry<String, Object> e : config.entrySet()) {
        _state.put(e.getKey(), Utils.serialize(e.getValue()));
      }
    }
  }
  
  
  

  @SuppressWarnings("unchecked")
  @Override
  public <T> T getState(String key, T defaultValue) {
    // _log.debug("get state: " + key);
    if (_state.containsKey(key)) {
      return (T) Utils.deserialize( _state.get(key) );
    }
    return defaultValue;
  }

  @Override
  public <T> void setState(final String key, final T val) {
    //_log.debug("set state: " + key + " " + val);

    synchronized(_state) {
      _state.put(key,  Utils.serialize(val));
    }

  }

  @Override
  public void initialize() {
  }

  
  @Override
  public void shutdown() {
//    clear();
  }

  
  @Override
  public void removeStateWithPrefix(String pattern) {
    synchronized(_state) {
      for(Entry<String, byte[]> e : new HashSet<Entry<String, byte[]>>(_state.entrySet())) {
        if (Utils.matchGlob(e.getKey(), pattern)) {
          //_log.debug("removing state : " + e.getKey());
          _state.remove(e.getKey());
        }
      }
    }
  }

  @Override
  public boolean hasState(String key) {
    return _state.containsKey(key);
  }


  public List<Pair<String, Object>> queryStates(String query) {
    List<Pair<String, Object>> list = Lists.newArrayList();
    synchronized (_state) {
      for(Entry<String, byte[]> e : new HashSet<Entry<String, byte[]>>(_state.entrySet())) {
        if (Utils.matchGlob(e.getKey(), query)) {
          list.add(new Pair<String, Object>(e.getKey(), Utils.deserialize(e.getValue())));
        }
      }  
    }
    
    return list;
  }

  @Override
  public com.zillabyte.motherbrain.coordination.Lock lock(final String name, long timeout, long duration) {
    synchronized(_locks) {
      if (_locks.containsKey(name) == false) {
//        _log.info("lock doesn't exist for "+name+" yet. putting in hash.");
        _locks.put(name, new ReentrantLock());
      }
    }
//    _log.info("trying to lock: " + name);
    _locks.get(name).lock();
//    _log.info("lock success: " + name);
    
    return(new com.zillabyte.motherbrain.coordination.Lock() {
      @Override
      public void release() throws CoordinationException {
        Lock l = _locks.get(name);
        if (l == null) throw new NullPointerException("tried to unlock non existant lock: " + name);

//        _log.info("unlock: " + name);        
        try {
          l.unlock();
        } catch (IllegalMonitorStateException e) {
          _log.warn("illegal unlock: " + e);
        }
//          _log.info("lock released: " + name);
      }
    });
    
  }


  @Override
  public void clear() {
    synchronized(_state) {
      _state.clear();
    }
    synchronized(_messageWatchers) {
      _messageWatchers.clear();
    }
    synchronized(_locks) {
      _locks.clear();
    }
  }
  

  @Override
  public void sendMessage(final String stream, final Object message) {
    maybeDefer(new Runnable() {

      @Override
      public void run() {
        for(Pair<String, MessageHandler> p : globSelect(stream, _messageWatchers)) {
//          _log.info("sending message: " + stream + " obj: " + message);
          try {
            handleNewMessage(stream, message, p.getValue1());
          } catch (Exception e) {
            e.printStackTrace();
          }
        }        
      }
      
    });
    
  }

  @Override
  public Watcher watchForMessage(String stream, MessageHandler messageHandler) {
    synchronized(_messageWatchers) {
      Pair<String, MessageHandler> p = new Pair<String, MessageHandler>(stream, messageHandler);
      _messageWatchers.add(p);
      return new MessageWatcher(p);
    }
  }

  
  
  /***
   * Use to (optionally) defer all callbacks
   * @param fn
   */
  private void maybeDefer(final Runnable fn) {
    Utils.schedule(_defer, fn);
  }
  
  
  
  /***
   * Used to select patterns based on globs
   * @param key
   * @param list
   * @return
   */
  private <T> List<Pair<String, T>> globSelect(String key, List<Pair<String, T>> list) {
    List<Pair<String, T>> retList = Lists.newArrayList();
    synchronized(list) {
      for(Pair<String, T> p : list) {
        if (Utils.matchGlob(key, p.getValue0())) {
          retList.add(p);
        }
      }
    }
    return retList;
  }

  
  
  public int getNumberOfMessageListeners(String stream) {
    return globSelect(stream, _messageWatchers).size();
  }

  @Override
  public void sendTransactionalMessage(String channel, Object message, long timeout) throws CoordinationException, TimeoutException {
    _askWrapper.sendTransactionalMessage(channel, message, timeout);
  }

  @Override
  public Watcher watchForAsk(String channel, AskHandler askHandler) throws CoordinationException {
    return _askWrapper.watchForAsk(channel, askHandler);
  }

  @Override
  public Object ask(String channel, Object message, long timeout) throws CoordinationException, TimeoutException {
    return _askWrapper.ask(channel, message, timeout);
  }

  @Override
  public void handleNewMessage(String key, Object message, MessageHandler handler) throws CoordinationException {
    _askWrapper.handleNewMessage(key, message, handler);
  }
  
}


