package grandmotherbrain.flow.collectors.coordinated.support;

import java.util.Iterator;

import org.javatuples.Triplet;

public interface TupleIdMapper extends Iterable<Triplet<Integer, Object, TupleIdSet>> {

  public Iterator<Triplet<Integer, Object, TupleIdSet>> iterator();

  public void removeTupleIdValues(TupleIdSet tupleIds);

  public void removeTupleIdKey(Object key);

  public void addEntry(Integer downstreamTask, Object object, Object id);

  public void ensureEntry(Integer upstreamTask, Object originTuple);

  public void setTimestamp(TupleIdSet set, long currentTimeMillis);

  public int getTupleSize();

  
}
