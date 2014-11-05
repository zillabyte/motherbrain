package grandmotherbrain.flow.operations;

import grandmotherbrain.flow.Fields;
import grandmotherbrain.flow.MapTuple;
import grandmotherbrain.flow.aggregation.AggregationException;
import grandmotherbrain.flow.aggregation.AggregationKey;
import grandmotherbrain.flow.aggregation.AggregationStoreWrapper;
import grandmotherbrain.flow.aggregation.Aggregator;
import grandmotherbrain.flow.collectors.OutputCollector;
import grandmotherbrain.flow.collectors.coordinated.CoordinatedOutputCollector;
import grandmotherbrain.flow.config.OperationConfig;
import grandmotherbrain.flow.error.strategies.FakeLocalException;
import grandmotherbrain.top.MotherbrainException;
import grandmotherbrain.universe.Universe;
import grandmotherbrain.utils.Log4jWrapper;
import grandmotherbrain.utils.SerializableMonitor;
import grandmotherbrain.utils.Utils;

import java.util.Iterator;

public class GroupBy extends AggregationOperation {

  private static final long serialVersionUID = 2734755611608351415L;

  private final SerializableMonitor _aggGroupMutex;
  protected volatile Aggregator _aggregator;
  protected final AggregationStoreWrapper _store;

  private Log4jWrapper _log = Log4jWrapper.create(GroupBy.class, this);

  
  /***
   * 
   * @param name
   * @param groupFields
   * @param aggregator
   */
  public GroupBy(final String name, final Fields groupFields, Aggregator aggregator) {
    super(name);
    super.setIncomingRouteByFields(groupFields);
    this._aggregator = aggregator;
    this._store = Universe.instance().aggregationStoreFactory().getStore(this, "");
    this._aggGroupMutex = new SerializableMonitor();
  }
  
  
  /***
   * 
   * @param name
   * @param groupFields
   */
  public GroupBy(final String name, final Fields groupFields) {
    this(name, groupFields, OperationConfig.createEmpty());
  }
  
  
  public GroupBy(String name, Fields groupFields, OperationConfig config) {
    super(name, config);
    super.setIncomingRouteByFields(groupFields);
    this._store = Universe.instance().aggregationStoreFactory().getStore(this, "");
    this._aggGroupMutex = new SerializableMonitor();
  }


  /***
   * 
   */
  @Override
  public void handleEmit(Object batchId, Integer aggKeyStore) throws InterruptedException, OperationException, OperationDeadException {
    try {
      // Start Aggregating
      aggrgateAllGroups(batchId, aggKeyStore, _collector);
    } catch (AggregationException e) {
      throw new OperationException(this, e);
    }
  }
  
  
  

  /****
   * 
   * @param key
   * @param c
   * @throws InterruptedException 
   * @throws OperationException 
   * @throws OperationDeadException 
   * @throws AggregationException 
   */
  private void aggregateGroup(Object batch, Integer aggKeyStore, AggregationKey key, OutputCollector c) throws InterruptedException, OperationException, OperationDeadException, AggregationException {
    try {
      // Don't allow multiple aggregations happen on the same thread at the same time. 
      synchronized(_aggGroupMutex) {
        
        // Init
        //_log.info("beginning aggregation for: " + key);
        if (_store.hasGroup(iterationStoreKeyPrefix(batch, aggKeyStore), key) == false) {
          _log.warn("aggergateGroup called for non existant group: " + key);
          return;
        }
        
        incLoop();
        markBeginActivity();
        try {
          
          // Build the group Maptuple
          MapTuple groupTuple = this.buildTupleFromKey(this.getIncomingRouteByFields(), key);
          
          // synchronize so that the output collector (and in particular the sub-batch) cannot be changed during the aggregation phase
          synchronized(c) {

            // Is this part of a batch? 
            if (c instanceof CoordinatedOutputCollector) {
              ((CoordinatedOutputCollector)c).setCurrentBatch(batch);
            }

            // Step 1: Call the start operation...
            if (_outerAggregateLogBackoff.tick()) {
              _operationLogger.writeLog("[sampled #" + _outerAggregateLogBackoff.counter() +"] beginning group on: " + groupTuple, OperationLogger.LogPriority.IPC);
            }
           
            _aggregator.start(groupTuple);
            
            // Step 2: Aggregate all the values...
            Iterator<MapTuple> iter = _store.getGroupIterator(iterationStoreKeyPrefix(batch, aggKeyStore), key);
            while(iter.hasNext()) {
              MapTuple t = iter.next();
              if (_innerAggregateLogBackoff.tick()) {
                _operationLogger.writeLog("[sampled #" + _innerAggregateLogBackoff.counter() +"] aggregating tuple: " + t, OperationLogger.LogPriority.IPC);
              }
              _aggregator.aggregate(t, c);
            }
            
            // Step 3: Send complete signal...
            _aggregator.complete(c);
          }
          
        } catch (MotherbrainException ex) {
          handleLoopError(ex);
          
        } catch (InterruptedException ex) {
          throw ex;
          
        } catch(Throwable e) {
          handleFatalError(e);
          
        } finally {
          // Tell the store we can release its state
          markEndActivity();
          _store.deleteGroup(iterationStoreKeyPrefix(batch, aggKeyStore), key);
        }
      }
    } catch(FakeLocalException e) {
      e.printAndWait();
    }
  }

  /***
   * 
   * @param c
   * @throws InterruptedException 
   * @throws OperationException 
   * @throws OperationDeadException 
   * @throws AggregationException 
   */
  private void aggrgateAllGroups(Object batchId, Integer aggStoreKey, OutputCollector c) throws InterruptedException, OperationException, OperationDeadException, AggregationException {
    
    // Init
    final Iterator<AggregationKey> iter;
    iter = _store.keyIterator(iterationStoreKeyPrefix(batchId, aggStoreKey));
    
    _log.info("Aggregating all groups..");
    while(iter.hasNext()) {
      if (inPressureState()) {
        // Spin wait while we allow pressure to die down. Note: this is not the main thead. 
        Utils.sleep(100L);
        continue;
      }
      aggregateGroup(batchId, aggStoreKey, iter.next(), c);
    }
    
    // Done
    _log.info("Aggregating all groups done.");
  }

  



  /**
   * @throws AggregationException *
   * 
   */
  @Override
  public void handleConsume(Object batch, MapTuple t, String sourceStream, OutputCollector c) throws AggregationException {
    _store.addToGroup(storeKeyPrefix(batch), this.getKey(this.getIncomingRouteByFields(), t), t);
  }

  
  /**
   * 
   */
  @Override
  public String type() {
    return "group_by";
  }

  


}
