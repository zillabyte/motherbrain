package grandmotherbrain.flow.operations.multilang.operations;

import grandmotherbrain.container.ContainerWrapper;
import grandmotherbrain.flow.Fields;
import grandmotherbrain.flow.MapTuple;
import grandmotherbrain.flow.aggregation.Aggregator;
import grandmotherbrain.flow.collectors.OutputCollector;
import grandmotherbrain.flow.operations.GroupBy;
import grandmotherbrain.flow.operations.OperationException;
import grandmotherbrain.flow.operations.multilang.MultiLangException;
import grandmotherbrain.top.MotherbrainException;
import net.sf.json.JSONObject;

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
    _handler.generalObserver().mabyeThrowNextError();
    
  }

  
  @Override
  public void aggregate(MapTuple t, OutputCollector collector) throws MotherbrainException, InterruptedException {
    
    _handler.ensureAlive();
    _handler.addAliases(t);
    _handler.generalObserver().sendAggregate(t, null);
    _handler.generalObserver().mabyeThrowNextError();
    _handler.tupleObserver().waitForDoneMessageWithoutCollecting();
    
  }

  
  @Override
  public void complete(OutputCollector c) throws MotherbrainException, InterruptedException {

    //send command end_group
    _handler.generalObserver().sendEndGroup();
    _handler.tupleObserver().collectTuplesUntilDone(c);
    _handler.generalObserver().mabyeThrowNextError();
    
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
