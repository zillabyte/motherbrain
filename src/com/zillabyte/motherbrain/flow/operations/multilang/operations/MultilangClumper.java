package com.zillabyte.motherbrain.flow.operations.multilang.operations;

import java.util.List;

import net.sf.json.JSONObject;

import com.zillabyte.motherbrain.container.ContainerWrapper;
import com.zillabyte.motherbrain.flow.MapTuple;
import com.zillabyte.motherbrain.flow.collectors.OutputCollector;
import com.zillabyte.motherbrain.flow.operations.OperationException;
import com.zillabyte.motherbrain.flow.operations.builtin.Clumper;
import com.zillabyte.motherbrain.flow.operations.multilang.MultiLangProcessException;

public class MultilangClumper extends Clumper {

  
  private MultilangHandler _handler;
  private long _process_timeout;
  
  
  public MultilangClumper(JSONObject node, ContainerWrapper container) {
    super(MultilangHandler.getName(node), Integer.parseInt(MultilangHandler.getConfig(node, "size", "100")));
    _handler = new MultilangHandler(this, node, container);
  }


  @Override
  public void prepare() throws OperationException, InterruptedException {
    _handler.prepare();
    super.prepare();
  }

  
  @Override
  public void cleanup() throws InterruptedException, OperationException {
    _handler.cleanup();
    super.cleanup();
  }

  
  
  @Override 
  public boolean isAlive() {
    return _handler.isAlive();
  }

  
  
  @Override
  public void onFinalizeDeclare() throws OperationException, InterruptedException {
    super.onFinalizeDeclare();
    _handler.onFinalizeDeclare();
  }


  @Override
  public void execute(List<MapTuple> tuples, OutputCollector collector) throws OperationException {
    try { 
      
      _handler.ensureAlive();
      _handler.generalObserver().sendTuplesMessage(tuples);
      _handler.tupleObserver().collectTuplesUntilDone(collector);
      _handler.generalObserver().maybeThrowNextError();  
    
    } catch (MultiLangProcessException | InterruptedException e) {
      throw new OperationException(this,e);
    }
  }

  
}
