package grandmotherbrain.flow.operations;

import grandmotherbrain.flow.Fields;
import grandmotherbrain.flow.MapTuple;
import grandmotherbrain.flow.aggregation.AggregationException;
import grandmotherbrain.flow.aggregation.AggregationKey;
import grandmotherbrain.flow.aggregation.AggregationStoreWrapper;
import grandmotherbrain.flow.collectors.OutputCollector;
import grandmotherbrain.flow.collectors.coordinated.BatchedTuple;
import grandmotherbrain.flow.config.OperationConfig;
import grandmotherbrain.flow.error.strategies.FakeLocalException;
import grandmotherbrain.flow.graph.Connection;
import grandmotherbrain.relational.MissingFieldException;
import grandmotherbrain.top.MotherbrainException;
import grandmotherbrain.universe.Universe;
import grandmotherbrain.utils.Log4jWrapper;
import grandmotherbrain.utils.Utils;

import java.util.Iterator;

import net.sf.json.JSONObject;

import org.apache.commons.lang.NotImplementedException;

public class Join extends AggregationOperation {
  
  private static final long serialVersionUID = -2683557258333764627L;
  
  private JoinType _joinType = JoinType.INNER;
  private Fields _lhsGroupFields;
  private Fields _rhsGroupFields;
  private String _lhsStream;
  private String _rhsStream;
  private AggregationStoreWrapper _lhsStore;
  private AggregationStoreWrapper _rhsStore;

  private Log4jWrapper _log = Log4jWrapper.create(Join.class, this);
  
  

  public Join(final String name, String lhsStream, Fields lhsGroupFields, String rhsStream, Fields rhsGroupFields, JoinType joinType, OperationConfig config) {
    super(name, config);
    _lhsStream = lhsStream;
    _rhsStream = rhsStream;
    _lhsGroupFields = lhsGroupFields;
    _rhsGroupFields = rhsGroupFields;
    _lhsStore = Universe.instance().aggregationStoreFactory().getStore(this, "lhs");
    _rhsStore = Universe.instance().aggregationStoreFactory().getStore(this, "rhs");
    _joinType = joinType;
  }


  public Join(JSONObject node) {
    this(
        node.getString("name"),
        node.getString("lhs_stream"),
        new Fields(node.getString("lhs_fields")),
        node.getString("rhs_stream"),
        new Fields(node.getString("rhs_fields")),
        JoinType.valueOf(node.getString("join_type").toUpperCase()),
        OperationConfig.createEmpty()
        );
  }


  /***
   * 
   */
  @Override
  public String type() {
    return "join";
  }


  
  /***
   * 
   * @param streamName
   */
  public void setLhsStreamName(String streamName) {
    _lhsStream = streamName;
  }
  
  
  /***
   * 
   * @param streamName
   */
  public void setRhsStreamName(String streamName) {
    _rhsStream = streamName;
  }



  /**
   * @throws InterruptedException 
   * @throws OperationException 
   * @throws OperationDeadException **
   * 
   */
  @Override
  public void handleEmit(Object batch, Integer aggStoreKey) throws InterruptedException, OperationException, OperationDeadException {
    try {
     
      // Init
      _log.info("Aggregating all groups..");
      final Iterator<AggregationKey> iter;
      switch(this._joinType) {
      case INNER:
        /* fall through */
      case LEFT:
        iter = this._lhsStore.keyIterator(iterationStoreKeyPrefix(batch, aggStoreKey));
        break;
      case RIGHT:
        iter = this._rhsStore.keyIterator(iterationStoreKeyPrefix(batch, aggStoreKey));
        break;
      case OUTER:
        // See below
        throw new OperationException(this, new NotImplementedException());
      default:
        throw new OperationException(this, "unknown join type");
      }
  
      while(iter.hasNext()) {
        AggregationKey key = iter.next();
        performJoin(iterationStoreKeyPrefix(batch, aggStoreKey), key);
      }
  
      // Done
      _log.info("Aggregating all groups done.");
    } catch(AggregationException e) {
      throw new OperationException(this, e);
    }
  }

  
  /***
   * 
   * @param lhs
   * @param rhs
   */
  private final MapTuple joinTuples(MapTuple lhs, MapTuple rhs) {
    
    // Create the new tuple
    MapTuple joinedTuple = new MapTuple();
    if (lhs != null) joinedTuple.values().putAll(lhs.values());
    if (rhs != null) joinedTuple.values().putAll(rhs.values());

    // Because we're dealing with LEFT,RIGHT, and OUTER joins, it's possible 
    // to have NULLs; To allow the rest of the system to function as expected, we 
    // cheat here a bit and add any missing fields to the map
    if (_joinType != JoinType.INNER) {
      if (this._expectedFields.size() > 0) {
        Fields expected = this._expectedFields.values().iterator().next();
        for(final String s : expected) {
          /*
           * We should never insert null values into the _expectedFields hash.
           */
          assert (s != null);
          if (joinedTuple.containsValueKey(s) == false) {
            joinedTuple.put(s, null);
          }
        }
      }
    }
    
    // Batched tuples?
    MapTuple inc = lhs != null ? lhs : rhs;  
    if (inc instanceof BatchedTuple) {
      BatchedTuple bt = (BatchedTuple) inc;
      joinedTuple = new BatchedTuple(joinedTuple, bt.getId(), bt.batchId());
    }
    
    return joinedTuple;
  }
  
  
  @Override
  public void onSetExpectedFields() throws OperationException {
    final String lhsStream = this.lhsPrevConnection().streamName();
    final String rhsStream = this.rhsPrevConnection().streamName();
    /*
     * Bug in nullness annotation derivations, this is true for sure.
     */
    assert (lhsStream != null);
    assert (rhsStream != null);
    this.lhsPrevOperation().addExpectedFields(lhsStream, this._lhsGroupFields);
    this.rhsPrevOperation().addExpectedFields(rhsStream, this._rhsGroupFields);
    super.onSetExpectedFields();
  }
  

  /***
   * 
   * @param key
   * @throws InterruptedException 
   * @throws OperationException 
   * @throws OperationDeadException 
   * @throws AggregationException 
   */
  private synchronized void performJoin(String fullBatchName, AggregationKey key) throws InterruptedException, OperationException, OperationDeadException, AggregationException {
    try {
      // Init
      _log.info("beginning join for: " + key);
      
      try {
        
        // Init 
        markBeginActivity();
          
        // What join are we doing? 
        if (_joinType == JoinType.LEFT) {
          
          Iterator<MapTuple> lhsIter = _lhsStore.getGroupIterator(fullBatchName, key);
          while(lhsIter.hasNext()) {
            
            // Pressure sanity...
            if (inPressureState()) {
              // Spin wait while we allow pressure to die down. Note: this is not the main thead. 
              Utils.sleep(100L);
              continue;
            }
            
            // Init. 
            Iterator<MapTuple> rhsIter = _rhsStore.getGroupIterator(fullBatchName, key);
            MapTuple lhsTuple = lhsIter.next();
            
            // If nothing on other side, then just emit lhs tuple
            if (rhsIter.hasNext() == false) {
              _collector.emit(joinTuples(lhsTuple, null));
            } else {
              while(rhsIter.hasNext()) {
                _collector.emit(joinTuples(lhsTuple, rhsIter.next()));
              }
            }
          }
          
        } else if (_joinType == JoinType.RIGHT) {
          
          // Same as LEFT join, but swap sides
          Iterator<MapTuple> rhsIter = _rhsStore.getGroupIterator(fullBatchName, key);
          while(rhsIter.hasNext()) {
            
            // Pressure sanity...
            if (inPressureState()) {
              // Spin wait while we allow pressure to die down. Note: this is not the main thead. 
              Utils.sleep(100L);
              continue;
            }
            
            // Init. 
            Iterator<MapTuple> lhsIter = _lhsStore.getGroupIterator(fullBatchName, key);
            MapTuple rhsTuple = rhsIter.next();
            
            // If nothing on other side, then just emit lhs tuple
            if (lhsIter.hasNext() == false) {
              _collector.emit(joinTuples(null, rhsTuple));
            } else {
              while(lhsIter.hasNext()) {
                _collector.emit(joinTuples(lhsIter.next(), rhsTuple));
              }
            }
          }
          
        } else if (_joinType == JoinType.INNER) {
        
          // Only emit tuples where matches on boths ides
          Iterator<MapTuple> lhsIter = _lhsStore.getGroupIterator(fullBatchName, key);
          while(lhsIter.hasNext()) {
            
            // Pressure sanity...
            if (inPressureState()) {
              // Spin wait while we allow pressure to die down. Note: this is not the main thead. 
              Utils.sleep(100L);
              continue;
            }
            
            // Init. 
            MapTuple lhsTuple = lhsIter.next();
            Iterator<MapTuple> rhsIter = _rhsStore.getGroupIterator(fullBatchName, key);
            
            // Iterate every possiblity..
            while(rhsIter.hasNext()) {
              _collector.emit(joinTuples(lhsTuple, rhsIter.next()));
            }
          }
        
        } else if (_joinType == JoinType.OUTER) {
         
          throw new NotImplementedException("TODO");
          
          // The outer join isn't necessarily complex, but 'done is better than perfect', and I'd
          // rather come back to it when we have use cases demanding it. Future notes: 
          // - Just iterate lhs side and copy the LEFT join code above
          // - Then (nested) iterate the rhs side and pretty much copy the RIGHT join code above as well. 
          
        }
        
      } catch (MotherbrainException ex) {
        handleLoopError(ex);
      } catch(Throwable e) {
        handleFatalError(e);
      } finally {
        // Tell the store we can release it's state
        _lhsStore.deleteGroup(fullBatchName, key);
        _rhsStore.deleteGroup(fullBatchName, key); 
        markEndActivity();
        
      }
    } catch(FakeLocalException e) {
      e.printAndWait();
    }
  }

  
  

  /**
   * @throws InterruptedException 
   * @throws OperationException
   * @throws MissingFieldException
   * @throws AggregationException 
   * 
   */
  @Override
  public void handleConsume(Object batch, MapTuple tuple, String sourceStream, OutputCollector c) throws InterruptedException, MotherbrainException {
    // Determine what stream we're working with... 
    if (sourceStream.equalsIgnoreCase(this._lhsStream)) {
      // LHS
      if (_outerAggregateLogBackoff.tick()) {
        _operationLogger.writeLog("[sampled #" + _outerAggregateLogBackoff.counter() +"] receiving LHS join tuple: " + tuple, OperationLogger.LogPriority.IPC);
      }
      this._lhsStore.addToGroup(storeKeyPrefix(batch), this.getKey(this._lhsGroupFields, tuple), tuple);
    } else if (sourceStream.equalsIgnoreCase(this._rhsStream)) {
      // RHS
      if (_outerAggregateLogBackoff.tick()) {
        _operationLogger.writeLog("[sampled #" + _outerAggregateLogBackoff.counter() +"] receiving RHS join tuple: " + tuple, OperationLogger.LogPriority.IPC);
      }
      this._rhsStore.addToGroup(storeKeyPrefix(batch), this.getKey(this._rhsGroupFields, tuple), tuple);
    } else {
      throw new OperationException(this, "unknown stream: " + sourceStream).setUserMessage("The stream '"+sourceStream+"' does not exist for the operation '"+namespaceName()+"'."); 
    }
  }


  public Fields lhsJoinFields() {
    return _lhsGroupFields;
  }
  
  public Fields rhsJoinFields() {
    return _rhsGroupFields;
  }


  public Operation lhsPrevOperation() {
    return lhsPrevConnection().source();
  }


  public Operation rhsPrevOperation() {
    return rhsPrevConnection().source();
  }
  
  
  public Connection lhsPrevConnection() {
    for(Connection c : this.getTopFlow().graph().nonLoopConnectionsTo(this)) {
      if (c.streamName().equals(this._lhsStream)) {
        return c;
      }
    }
    throw new IllegalStateException("cannot find join stream!");
  }
  
  
  public Connection rhsPrevConnection() {
    for(Connection c : this.getTopFlow().graph().nonLoopConnectionsTo(this)) {
      if (c.streamName().equals(this._rhsStream)) {
        return c;
      }
    }
    throw new IllegalStateException("cannot find join stream!");
  }


  @Override
  public Operation prevNonLoopOperation() throws OperationException {
    throw new OperationException(this, "the caller must special-case for joins");
  }
}
