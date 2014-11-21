package com.zillabyte.motherbrain.flow.aggregation;

import java.util.Collection;
import java.util.Iterator;

import org.apache.log4j.Logger;

import com.zillabyte.motherbrain.flow.MapTuple;
import com.zillabyte.motherbrain.flow.operations.AggregationOperation;
import com.zillabyte.motherbrain.flow.operations.LoopException;
import com.zillabyte.motherbrain.universe.Config;
import com.zillabyte.motherbrain.utils.Utils;


/***
 *  Production Aggregate Store using Both DiskBacked and Memory stores
 *  Caches writes to map in memory and flushes 
 *  @author sjarvie
 */
public class CachedStore implements AggregationStore {

  private static final long serialVersionUID = -1837276500478667648L;
  private AggregationStore _store;
  
  // TODO: disk-backed aggregation seems to cave when put under stress.  TODO: remove the MAX_VALUE when done. 
  private long _limit = Config.getOrDefault("aggregation.memory.store.limit", Integer.MAX_VALUE); // an approximate limit to amount of bytes worth of tuples to store in memory
  
  private long _currentSize = 0;
  private AggregationOperation _operation;
  private String _extraPrefix;
  private static Logger _log = Utils.getLogger(CachedStore.class);


  /***
   * 
   * @param o
   */
  public CachedStore(AggregationOperation o) {
    this(o, "");
  }
  

  /***
   * Joins utilize extraPrefix to seperate left and right and sides
   * @param o
   * @param extraPrefix
   */
  public CachedStore(AggregationOperation o, String extraPrefix) {
    _operation = o;
    _extraPrefix = extraPrefix;
    _store = new MemoryStore();  // start out with MemoryStore
  }

  
  
  /**
   * Set the limit for caching tuples before offloading
   * @param limit
   */
  public void setLimit(int limit){
    this._limit = limit;
  }

  

  /***
   * 
   */
  @Override
  public synchronized void addToGroup(Object batch, AggregationKey key, MapTuple tuple) throws LoopException {
    _store.addToGroup(batch, key, tuple);
    maybeSwitch(tuple);
  }



  /***
   * 
   */
  @Override
  public synchronized boolean hasGroup(Object batch, AggregationKey key) throws LoopException {
    return _store.hasGroup(batch, key);
  }


  /***
   * 
   */
  @Override
  public synchronized Iterator<MapTuple> getGroupIterator(Object batch, AggregationKey key) throws LoopException {
    return _store.getGroupIterator(batch, key);
  }


  /***
   * 
   */
  @Override
  public synchronized void deleteGroup(Object batch, AggregationKey key) throws LoopException {
    _store.deleteGroup(batch, key);
  }
  
  
  /**
   * Move contents from memory to disk
   * @throws AggregationException  
   */
  public synchronized void flush(Object batch) throws LoopException {
    _store.flush(batch);
  }


  
  /**
   * 
   */
  @Override
  public synchronized Iterator<AggregationKey> keyIterator(Object batch) throws LoopException {
    return _store.keyIterator(batch);
  }
  


  /***
   * 
   */
  @Override
  public void deleteBatch(Object batch) throws LoopException {
    _store.deleteBatch(batch);
  }
  
  /***
   * 
   * @param tuple
   * @throws AggregationException 
   */
  private synchronized void maybeSwitch(MapTuple tuple) throws LoopException {
    
    _currentSize += tuple.getApproxMemSize();
    
    if (_currentSize > _limit && this._store instanceof MemoryStore) {
      
      // Init 
      _log.info("switching to DiskBackedAggregator...");
      DiskBackedStore diskStore = new DiskBackedStore(_operation, _extraPrefix);
      Collection<Object> batches = ((MemoryStore)_store).getBatches();
      
      for(Object batch : batches) {
        Iterator<AggregationKey> keyIter = _store.keyIterator(batch);
        while(keyIter.hasNext()) {
          AggregationKey key = keyIter.next();
          Iterator<MapTuple> tupleIter = _store.getGroupIterator(batch, key);
          while(tupleIter.hasNext()) {
            // Sink into the new store... 
            diskStore.addToGroup(batch, key, tupleIter.next());
          }
        }
      }
      
      // Switch over...
      _store = diskStore;
    }
  }

  
  /***
   * 
   * @return
   */
  public boolean isDiskStore() {
    return _store instanceof DiskBackedStore;
  }
  
  
  /***
   * 
   * @return
   */
  public AggregationStore getStore() {
    return _store;
  }



}