package grandmotherbrain.flow.graph;

import grandmotherbrain.flow.operations.Operation;

import java.io.Serializable;

/****
 * Represents all the connections inf a component/app
 * @author jake
 *
 */
public final class Connection implements Serializable {

  /**
   * 
   */
  private final FlowGraph flowGraph;

  private static final long serialVersionUID = 5667910218342408671L;
  final public static Integer DEFAULT_MAX_ITER = 100;
  
  private final String _sourceId;
  private final String _destId; 
  private final String _streamName;
  private Boolean _loopBack = Boolean.FALSE;
  private int _maxIter = DEFAULT_MAX_ITER;
  
  public Connection(FlowGraph flowGraph, Operation source, Operation dest, String name, Boolean loopBack, Integer maxIter) {
    this.flowGraph = flowGraph;
    _sourceId = source.namespaceName();
    _destId = dest.namespaceName();
    _streamName = name;
    _loopBack = loopBack;
    _maxIter = Math.min(maxIter, DEFAULT_MAX_ITER);
  }
  
  public Connection(FlowGraph flowGraph, Operation source, Operation dest, String name) {
    this(flowGraph, source, dest, name, false, 0);
  }
  
  public String destId() {
    return _destId;
  }
  
  public String sourceId() {
    return _sourceId;
  }
  
  public Boolean loopBack() {
    return _loopBack;
  }
  
  public String streamName() {
    return _streamName;
  }
  
  public Integer maxIterations() {
    return _maxIter;
  }
  
  public Operation source() {
    final Operation source = this.flowGraph.getById(_sourceId);
    assert (source != null);
    return source;
  }
  
  public Operation dest() {
    final Operation dest = this.flowGraph.getById(_destId);
    assert (dest != null);
    return dest;
  }

}