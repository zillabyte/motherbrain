package grandmotherbrain.flow.operations.multilang.operations;


import grandmotherbrain.container.ContainerWrapper;
import grandmotherbrain.flow.MapTuple;
import grandmotherbrain.flow.collectors.OutputCollector;
import grandmotherbrain.flow.operations.Function;
import grandmotherbrain.flow.operations.OperationException;
import grandmotherbrain.flow.operations.multilang.MultiLangException;
import grandmotherbrain.flow.operations.multilang.MultiLangProcessException;
import net.sf.json.JSONObject;


public final class MultiLangRunEach extends Function implements MultiLangOperation {

  private static final long serialVersionUID = -6784282062699640773L;
  
  private MultilangHandler _handler;
  private long _process_timeout;
  
  
  public MultiLangRunEach(JSONObject nodeSettings, ContainerWrapper container) {
    super(MultilangHandler.getName(nodeSettings), MultilangHandler.getConfig(nodeSettings));
    _handler = new MultilangHandler(this, nodeSettings, container);
  }

  @Override
  public void prepare() throws MultiLangException {
    _handler.prepare();
  }

  
  @Override
  public void cleanup() throws MultiLangException, InterruptedException {
    _handler.cleanup();
  }


  @Override
  public void process(final MapTuple t, final OutputCollector collector) throws OperationException, InterruptedException {
    try { 
        
      _handler.ensureAlive();
      _handler.addAliases(t);
      _handler.generalObserver().sendTupleMessage(t);
      _handler.tupleObserver().collectTuplesUntilDone(collector);
      _handler.generalObserver().mabyeThrowNextError();  
    
    } catch (MultiLangProcessException e) {
      throw new OperationException(this,e);
    }
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
