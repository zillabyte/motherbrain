package grandmotherbrain.flow.operations.builtin;

import grandmotherbrain.flow.MapTuple;
import grandmotherbrain.flow.collectors.OutputCollector;
import grandmotherbrain.flow.operations.AggregationOperation;
import grandmotherbrain.flow.operations.OperationException;
import grandmotherbrain.flow.operations.multilang.MultiLangException;
import grandmotherbrain.top.MotherbrainException;
import grandmotherbrain.universe.Config;

import java.util.List;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Lists;


/***
 * A clumper just 'clumps' together a handful of tuples and sends them down to the underlying
 * aggregator. Examples (1):  say we want to crawl 100 domains at a time.. how do we get 100 domains
 * into the operation?  Use case (2): say we build a custom s3 sink, but we want to sink 10000 tuples
 * at a time. 
 * @author jake
 *
 */
public abstract class Clumper extends AggregationOperation {

  
  private static final long serialVersionUID = 4350272423230272489L;
  private LinkedListMultimap<Object, MapTuple> _clump = LinkedListMultimap.create();
  private Long _clumpMaxCount = Config.getOrDefault("clumper.default.max.count", 100L);
  private Long _clumpMaxInterval = Config.getOrDefault("clumper.default.max.interval", 100L);
  private long _lastEmitTime;
  
  
  /***
   * 
   * @param name
   */  
  public Clumper(String name) {
    super(name);
  }
  
  public Clumper(String name, int clumpCount) {
    this(name);
    _clumpMaxCount = (long) clumpCount;
  }

  
  /**
   * @throws InterruptedException *
   * 
   */
  @Override
  public void prepare() throws MultiLangException, InterruptedException {
    super.prepare();
    _clumpMaxCount = Long.parseLong(getLocalConfig().get("max_count", _clumpMaxCount.toString()));
    _clumpMaxInterval = Long.parseLong(getLocalConfig().get("max_interval_ms", _clumpMaxInterval.toString()));
  }
  


  /**
   * @throws OperationException **
   * 
   */
  @Override
  public void handleEmit(Object batch, Integer subBatch) throws InterruptedException, OperationException {
    try {
      emitClump(batch, subBatch);
    } catch (MotherbrainException e) {
      throw new OperationException(this, e);
    }
  }

  
  /**
   * @throws InterruptedException 
   * @throws MotherbrainException **
   * 
   */
  private synchronized void emitClump(Object batch, Integer aggStoreKey) throws InterruptedException, MotherbrainException {
    
    List<MapTuple> list = Lists.newLinkedList();
    for(MapTuple tuple : _clump.get(iterationStoreKeyPrefix(batch, aggStoreKey))) {
      list.add(tuple);
    }
    execute(list, this._collector);
    _clump.clear();
    _lastEmitTime = System.currentTimeMillis();
  }
  
  
  
  public abstract void execute(List<MapTuple> tuples, OutputCollector collector) throws OperationException;
  
  
  /***
   * 
   * @throws InterruptedException
   * @throws MotherbrainException 
   */
  private void maybeEmitClump(Object batch, Integer aggStoreKey) throws InterruptedException, MotherbrainException {
    if (_clump.size() >= _clumpMaxCount) {
      emitClump(batch, aggStoreKey);
    } else if (_clumpMaxInterval > 0 && _lastEmitTime + _clumpMaxInterval < System.currentTimeMillis()) {
      emitClump(batch, aggStoreKey);
    }
  }
  
  
  /***
   * 
   * @param t
   */
  private synchronized void addToClump(Object batch, MapTuple t) {
    _clump.put(storeKeyPrefix(batch), t);
  }


  @Override
  public void handleConsume(Object batch, MapTuple t, String sourceStream, OutputCollector c) throws MotherbrainException, InterruptedException {
    addToClump(batch, t);
    maybeEmitClump(batch, getIterationStoreKey(batch));
  }

}
