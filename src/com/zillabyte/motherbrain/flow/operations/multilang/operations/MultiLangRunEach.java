package com.zillabyte.motherbrain.flow.operations.multilang.operations;


import net.sf.json.JSONObject;

import com.zillabyte.motherbrain.container.ContainerWrapper;
import com.zillabyte.motherbrain.flow.MapTuple;
import com.zillabyte.motherbrain.flow.collectors.OutputCollector;
import com.zillabyte.motherbrain.flow.operations.Function;
import com.zillabyte.motherbrain.flow.operations.LoopException;


public final class MultiLangRunEach extends Function implements MultiLangOperation {

  private static final long serialVersionUID = -6784282062699640773L;
  
  private MultilangHandler _handler;
  private long _process_timeout;
  
  
  public MultiLangRunEach(JSONObject nodeSettings, ContainerWrapper container) {
    super(MultilangHandler.getName(nodeSettings), MultilangHandler.getConfig(nodeSettings));
    _handler = new MultilangHandler(this, nodeSettings, container);
  }

  @Override
  public void prepare() {
    _handler.prepare();
  }

  
  @Override
  public void cleanup() {
    _handler.cleanup();
  }


  @Override
  public void process(final MapTuple t, final OutputCollector collector) throws LoopException {

    _handler.ensureAlive();
    _handler.addAliases(t);
    _handler.generalObserver().sendTupleMessage(t);
    _handler.tupleObserver().collectTuplesUntilDone(collector);
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
  public void onFinalizeDeclare() {
    super.onFinalizeDeclare();
    _handler.onFinalizeDeclare();
  }

  
  @Override
  public MultilangHandler getMultilangHandler() {
    return _handler;
  }
  
}
