package grandmotherbrain.flow.aggregation;

import grandmotherbrain.flow.MapTuple;

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;


/***
 * A generic interface for storing aggregation values.  
 * 
 * @author jake
 *
 */
public interface AggregationStore extends Serializable {

  
  public final Object DEFAULT_BATCH = "_";
  public final Integer DEFAULT_SUB_BATCH = 0;


  /***
   * Add the given tuple to the specified group (key)
   * @param key
   * @param tuple
   */
  public void addToGroup(Object batch, AggregationKey key, MapTuple tuple) throws AggregationException;

  
  /***
   * Return T if the group exists
   * @param key
   */
  public boolean hasGroup(Object batch, AggregationKey key) throws AggregationException;

  
  /**
   * Returns an iterator that retuns all tuples belonging to the given key
   * Order is not guaranteed 
   * @param key
   */
  public Iterator<MapTuple> getGroupIterator(Object batch, AggregationKey key) throws AggregationException;
  
  
  /***
   * Deletes all data associated with the group
   * @param key
   */
  public void deleteGroup(Object batch, AggregationKey key) throws AggregationException;
  
  
  
  /***
   * 
   * @param batch
   * @throws AggregationException 
   */
  public void deleteBatch(Object batch) throws AggregationException;


  
  /***
   * Returns an iterator that lists all keys known to this store
   * @param batchId 
   * @throws InterruptedException 
   * @throws IOException 
   */
  public Iterator<AggregationKey> keyIterator(Object batch) throws AggregationException;
  
  
  /**
   * Flush store to offloader
   */
  public void flush(Object batch) throws AggregationException;
  
}
