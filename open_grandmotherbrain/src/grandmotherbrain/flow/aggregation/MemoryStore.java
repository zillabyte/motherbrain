package grandmotherbrain.flow.aggregation;

import grandmotherbrain.flow.MapTuple;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Maps;
import com.google.monitoring.runtime.instrumentation.common.com.google.common.collect.Sets;

/***
 * Use this for testing, not for production
 */
public class MemoryStore implements AggregationStore {

  private static final long serialVersionUID = -1837276500478667648L;
  
  private Map<Object, LinkedListMultimap<AggregationKey, MapTuple>> _map = Maps.newHashMap(); 

  
  private LinkedListMultimap<AggregationKey, MapTuple> getMap(Object batch) {
    if (_map.containsKey(batch) == false) {
      LinkedListMultimap<AggregationKey, MapTuple> innerMap = LinkedListMultimap.create(); 
      _map.put(batch, innerMap);
    }
    return _map.get(batch);
  }
  
  
  @Override
  public void deleteBatch(Object batch) {
    _map.remove(batch);
  }
  
  
  @Override
  public void addToGroup(Object batch, AggregationKey key, MapTuple tuple) {
    getMap(batch).put(key, tuple);
  }

  @Override
  public boolean hasGroup(Object batch, AggregationKey key) {
    return getMap(batch).containsKey(key);
  }

  @Override
  public Iterator<MapTuple> getGroupIterator(Object batch, AggregationKey key) {
    return getMap(batch).get(key).iterator();
  }

  @Override
  public void deleteGroup(Object batch, AggregationKey key) {
    getMap(batch).removeAll(key);
  }

  @Override
  public Iterator<AggregationKey> keyIterator(Object batch) {
    return Sets.newHashSet(getMap(batch).keySet()).iterator();
  }

  @Override
  public void flush(Object batch){
  }

  public Collection<Object> getBatches() {
    return this._map.keySet();
  }

  
}

