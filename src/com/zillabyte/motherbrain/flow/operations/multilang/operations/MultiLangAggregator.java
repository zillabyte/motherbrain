package com.zillabyte.motherbrain.flow.operations.multilang.operations;

import net.sf.json.JSONObject;

import com.zillabyte.motherbrain.container.ContainerWrapper;
import com.zillabyte.motherbrain.flow.Fields;
import com.zillabyte.motherbrain.flow.MapTuple;
import com.zillabyte.motherbrain.flow.aggregation.Aggregator;
import com.zillabyte.motherbrain.flow.collectors.OutputCollector;
import com.zillabyte.motherbrain.flow.operations.GroupBy;
import com.zillabyte.motherbrain.flow.operations.OperationException;
import com.zillabyte.motherbrain.flow.operations.multilang.MultiLangException;
import com.zillabyte.motherbrain.top.MotherbrainException;

public class MultiLangAggregator extends GroupBy implements Aggregator, MultiLangOperation {

  private static final long serialVersionUID = -9212729713297828051L;

  private MultilangHandler _handler;
  
  
  public MultiLangAggregator(JSONObject nodeSettings, ContainerWrapper container) {
    super(MultilangHandler.getName(nodeSettings), new Fields(nodeSettings.getJSONArray("group_by")), MultilangHandler.getConfig(nodeSettings));
    this._handler = new MultilangHandler(this, nodeSettings, container);
    this._aggregator = this;
  }
  

  
  
  
  @Override
  public void prepare() throws MultiLangException, InterruptedException {
    _handler.prepare();
  }
  

  @Override
  protected final void aggregationCleanup() throws MultiLangException, InterruptedException {
    _handler.cleanup();
  }
  
  
  @Override
  public void start(MapTuple t) throws MotherbrainException, InterruptedException {
    
    // send command begin_group
    _handler.ensureAlive();
    _handler.addAliases(t);
    _handler.generalObserver().sendBeginGroup(t);
    _handler.tupleObserver().waitForDoneMessageWithoutCollecting();
    _handler.generalObserver().maybeThrowNextError();
    
  }

  
  @Override
  public void aggregate(MapTuple t, OutputCollector collector) throws MotherbrainException, InterruptedException {
    
    _handler.ensureAlive();
    _handler.addAliases(t);
    _handler.generalObserver().sendAggregate(t, null);
    _handler.generalObserver().maybeThrowNextError();
    _handler.tupleObserver().waitForDoneMessageWithoutCollecting();
    
  }

  
  @Override
  public void complete(OutputCollector c) throws MotherbrainException, InterruptedException {

    //send command end_group
    _handler.generalObserver().sendEndGroup();
    _handler.tupleObserver().collectTuplesUntilDone(c);
    _handler.generalObserver().maybeThrowNextError();
    
  }
  
  @Override 
  public boolean isAlive() {
    return _handler.isAlive();
  }



  @Override
  public ContainerWrapper getContainer() {
    return _handler.getContainer();
  }


  
  @Override
  public void onFinalizeDeclare() throws OperationException, InterruptedException {
    super.onFinalizeDeclare();
    _handler.onFinalizeDeclare();
  }





  @Override
  public MultilangHandler getMultilangHandler() {
    return _handler;
  }
  
}
