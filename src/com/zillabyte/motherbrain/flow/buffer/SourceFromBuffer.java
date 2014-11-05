package com.zillabyte.motherbrain.flow.buffer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import net.sf.json.JSONObject;

import org.apache.commons.lang.NotImplementedException;
import org.apache.log4j.Logger;
import org.eclipse.jdt.annotation.NonNull;
import org.eclipse.jdt.annotation.Nullable;

import com.google.monitoring.runtime.instrumentation.common.com.google.common.base.Throwables;
import com.zillabyte.motherbrain.api.APIException;
import com.zillabyte.motherbrain.api.RelationsHelper;
import com.zillabyte.motherbrain.coordination.CoordinationException;
import com.zillabyte.motherbrain.flow.MapTuple;
import com.zillabyte.motherbrain.flow.StateMachineException;
import com.zillabyte.motherbrain.flow.collectors.OutputCollector;
import com.zillabyte.motherbrain.flow.collectors.coordinated.CoordinatedOutputCollector;
import com.zillabyte.motherbrain.flow.operations.OperationException;
import com.zillabyte.motherbrain.flow.operations.Source;
import com.zillabyte.motherbrain.flow.operations.multilang.MultiLangException;
import com.zillabyte.motherbrain.flow.tests.helpers.MockInstanceHelper;
import com.zillabyte.motherbrain.relational.AliasedQuery;
import com.zillabyte.motherbrain.relational.BufferQuery;
import com.zillabyte.motherbrain.relational.ColumnDef;
import com.zillabyte.motherbrain.relational.Query;
import com.zillabyte.motherbrain.universe.Config;
import com.zillabyte.motherbrain.universe.Universe;

public class SourceFromBuffer extends Source {

  private static final long serialVersionUID = 6624904012104574308L;
  public final int MAX_PARALLELISM = Config.getOrDefault("storm.stream.max.task.parallelism", 6);
  private static Logger _log = Logger.getLogger(SourceFromBuffer.class); 

  BufferConsumer _consumer;
  
  BufferQuery _query;
  
  private JSONObject _snapshot;

  public @Nullable Object _batch;
  public Integer _subBatch;
  
  protected String _bufferName;
  protected volatile Query _concreteQuery;

  /**
   * 
   * @param operationName            The operation ID/name
   * @param bufferName    The name of the buffer(e.g. Topic in Kafka) 
   */
  public SourceFromBuffer(String operationName, String bufferName, String flowId, String authToken) {

    super(operationName);
    _bufferName = bufferName;
    

    try {
      Query query = RelationsHelper.instance().concretifyQuery(flowId, bufferName, authToken);
      if(query instanceof BufferQuery){ 
        _query = (BufferQuery) query;
        Universe.instance().bufferService().maybeFillBuffer((BufferQuery) query);

      }
      else if(query instanceof AliasedQuery){
        _operationLogger.info("Unsupported dataset.");
        _log.error("Relation is on redshift. We have to offload it.");
        //TODO: Migrate redshift relation to kafka.

        throw new NotImplementedException("Cannot source from this relation at this time.");
      }else {
        _operationLogger.info("Unsupported dataset.");
        _log.error("Unsupported data set.");
        throw new NotImplementedException("Cannot source from this relation at this time.");

      }
      
    } catch (APIException | InterruptedException e) {
      Throwables.propagate(e);
    }
    _batch = 1;
  }

  @Override
  public void onBeginCycle(OutputCollector output) throws InterruptedException, OperationException, CoordinationException, StateMachineException, TimeoutException {
    super.onBeginCycle(output);
    if (_batch != null) {
      if (output instanceof CoordinatedOutputCollector) {
        ((CoordinatedOutputCollector)output).setCurrentBatch(_batch);
      }
    }
  }


  @Override
  protected synchronized boolean nextTuple(OutputCollector output) throws OperationException, InterruptedException {
    if(_consumer == null){
      _consumer = Universe.instance().bufferClientFactory().createConsumer(this);
    }

    MapTuple nextTuple = _consumer.getNextTuple();
    if (nextTuple != null) {
      output.emit(nextTuple);
      return true;
    } 

    // We'll let the consumer decide if it is done emitting.
    if(_consumer.isEmitComplete()){
      return false; // False signifies that the source is done, and flow can move into WFNC
    }
    else {
      return true; 
    }
  }


  @Override
  public void onFinalizeDeclare() throws OperationException, InterruptedException {
    super.onFinalizeDeclare();
    initConsumer(); 
  }


  private void initConsumer() {
    if (_snapshot != null){
      applySnapshot(_snapshot);
      _snapshot = null;
    }
  }

  public Query concreteQuery() throws OperationException, InterruptedException {
    final Query myConcreteQuery = this._concreteQuery; 
    if (myConcreteQuery == null) {
      synchronized (this) {
        final Query myNewConcreteQuery = this._concreteQuery;
        if (myNewConcreteQuery == null) {
          final Query concreteQuery;
          try {
            concreteQuery = RelationsHelper.instance().concretifyQuery(this.topFlowId(), _bufferName, this.getTopFlow().getFlowConfig().getAuthToken());
          } catch (APIException e) {
            throw new OperationException(this, e);
          }
          this._concreteQuery = concreteQuery;
          return concreteQuery;
        }
        return myNewConcreteQuery;
      }
    }
    return myConcreteQuery;
  }
  

  public String rawQuery(){
    return _bufferName;
  }
  

  public @NonNull List<ColumnDef> getValueColumnDefs() throws OperationException, InterruptedException {
    return concreteQuery().valueColumns();
  }

  @Override
  public Map<String, String> getAliases() throws OperationException, InterruptedException {
    final HashMap<String, String> map = new HashMap<>();
    for (ColumnDef cd : getValueColumnDefs()) {
      for (String alias : cd.getAliases()) {
        map.put(alias, cd.getName());
      }
    }
    return map;
  }




  @Override
  public void prepare() throws MultiLangException, InterruptedException {
    super.prepare();
    MockInstanceHelper.registerInstance(this);
  }


  @Override
  public int getMaxParallelism() {
    return MAX_PARALLELISM;
  }

  @Override
  public void handlePause() throws OperationException{
    super.handlePause();
  }



  @Override
  public JSONObject createSnapshot(){
    JSONObject snapshot = _consumer.createSnapshot();
    snapshot.put("batch", _batch);

    return snapshot;
  }

  @Override
  public void applySnapshot(JSONObject snapshot){
    _batch = Integer.valueOf(snapshot.getInt("batch"));

    if (_consumer != null) {
      _consumer.applySnapshot(snapshot);
    } else {
      // save the snapshot to be set upon _consumer startup
      _snapshot = snapshot;
    }   
  }

  public BufferQuery getQuery() {
    return _query;
  }


}
