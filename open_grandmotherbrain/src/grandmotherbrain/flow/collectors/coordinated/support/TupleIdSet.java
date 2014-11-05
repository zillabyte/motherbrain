package grandmotherbrain.flow.collectors.coordinated.support;

import java.io.Serializable;


/*****
 * A word of caution to implementers:  
 * 
 * Be aware that the concrete implementations of this interface MUST be robust & handle ***HUGE*** number of tuples.  
 *
 */
public interface TupleIdSet extends Serializable, Iterable<Object> {

  public final static TupleIdSet EMPTY = new EmptyTupleIdSet();
  
  void add(Object t);

  void addAll(TupleIdSet set);

  void removeAll(TupleIdSet set);

  int size();

  TupleIdSet getTupleIdsOlderThan(Long time);
  
  void setTimestamp(TupleIdSet set, Long time);
  
  Long getOldest();

  void cleanup();

  TupleIdSet clone();

  void remove(Object t);

  void clear();
  

}
