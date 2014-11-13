package com.zillabyte.motherbrain.flow;

import java.util.Collection;
import java.util.Map;

import org.eclipse.jdt.annotation.NonNullByDefault;

import com.zillabyte.motherbrain.flow.components.ComponentInput;
import com.zillabyte.motherbrain.flow.components.ComponentOutput;
import com.zillabyte.motherbrain.flow.config.OperationConfig;
import com.zillabyte.motherbrain.flow.graph.FlowGraph;
import com.zillabyte.motherbrain.flow.operations.AggregationOperation;
import com.zillabyte.motherbrain.flow.operations.Function;
import com.zillabyte.motherbrain.flow.operations.GroupBy;
import com.zillabyte.motherbrain.flow.operations.Join;
import com.zillabyte.motherbrain.flow.operations.JoinType;
import com.zillabyte.motherbrain.flow.operations.Operation;
import com.zillabyte.motherbrain.flow.operations.Sink;
import com.zillabyte.motherbrain.flow.operations.Source;
import com.zillabyte.motherbrain.flow.operations.decorators.RemoveFields;
import com.zillabyte.motherbrain.flow.operations.decorators.RenameFields;
import com.zillabyte.motherbrain.flow.operations.decorators.RetainFields;

@NonNullByDefault
public abstract class StreamBuilder {

  /****
   * 
   * @author jake
   *
   */
  public final static class AppStreamBuilder extends StreamBuilder {

    AppStreamBuilder(App flow, final Operation last, final String streamName) {
      super(flow, last, streamName);
    }

    public StreamBuilder sink(Sink fn) {
      _graph.connect(_last, fn, _streamName);
      _last = fn;
      fn.setContainerFlow(_flow);
      return this;
    }

    @Override
    public AppStreamBuilder extend(String streamName) {
      return new AppStreamBuilder((App)_flow, this._last, streamName);
    }
  }

  /****
   * 
   * @author jake
   *
   */
  public final static class ComponentStreamBuilder extends StreamBuilder {

    public ComponentStreamBuilder(Component flow, final Operation last, final String streamName) {
      super(flow, last, streamName);
    }

    public StreamBuilder outputs(ComponentOutput fn) {
      _graph.connect(_last, fn, _streamName);
      _last = fn;
      fn.setContainerFlow(_flow);
      return this;
    }

    @Override
    public StreamBuilder.ComponentStreamBuilder extend(String streamName) {
      return new ComponentStreamBuilder((Component)this._flow, this._last, streamName);
    }
  }

  public static final AppStreamBuilder makeAppStreamBuilder(final App flow, final Source source, final String streamName) {
    return new AppStreamBuilder(flow, source, streamName);
  }

  public static final AppStreamBuilder makeAppStreamBuilder(final App flow, final ComponentOutput source, final String streamName) {
    return new AppStreamBuilder(flow, source, streamName);
  }

  public static final ComponentStreamBuilder makeComponentStreamBuilder(final Component flow, final ComponentInput consumer, final String streamName) {
    return new ComponentStreamBuilder(flow, consumer, streamName);
  }

  public static final ComponentStreamBuilder makeComponentStreamBuilder(final Component flow, final ComponentOutput source, final String streamName) {
    return new ComponentStreamBuilder(flow, source, streamName);
  }
  
  protected FlowGraph _graph;
  protected Operation _last;
  protected String _streamName;
  protected Flow _flow;

  public StreamBuilder(Flow flow, Operation source, String streamName) {
    _graph = flow.graph();
    _flow = flow;
    _last = source;
    _streamName = streamName;
    source.setContainerFlow(flow);
    if (_flow == null) throw new NullPointerException("flow has not been set");
  }

  public StreamBuilder each(Function fn) {
    _graph.connect(_last, fn, _streamName);
    _last = fn;
    fn.setContainerFlow(_flow);
    return this;
  }
  
  public StreamBuilder each(Function fn, Boolean loopBack, Integer maxIter) {
    _graph.connect(_last, fn, _streamName, loopBack, maxIter);
    _last = fn;
    fn.setContainerFlow(_flow);
    return this;
  }
  

  public StreamBuilder renameFields(Map<String, String> rename) {
    _last.addEmitDecorator(_streamName, new RenameFields(rename));
    return this;
  }
  
  public StreamBuilder removeFields(Collection<String> remove) {
    _last.addEmitDecorator(_streamName, new RemoveFields(remove));
    return this;
  }
  
  public StreamBuilder retainFields(Collection<String> retain) {
    _last.addEmitDecorator(_streamName, new RetainFields(retain));
    return this;
  }
  
  
  public StreamBuilder aggregate(AggregationOperation fn) {
    _graph.connect(_last, fn, _streamName);
    _last = fn;
    fn.setContainerFlow(_flow);
    return this;
  }
  
  
  public StreamBuilder groupBy(GroupBy fn) {
    return aggregate(fn);
  }
  

  public StreamBuilder joinWith(StreamBuilder rawRhs, Join join) {
    StreamBuilder rhs = rawRhs;
    _graph.connect(_last, join, _streamName);
    _graph.connect(rhs._last, join, rhs._streamName);
    _last = join;
    join.setContainerFlow(_flow);
    return this;
  }
  

  public StreamBuilder joinWith(StreamBuilder rhs, Fields lhsJoinFields, Fields rhsJoinFields, JoinType joinType, OperationConfig config) {
    return joinWith(rhs, new Join("join." + this._streamName + "-" + rhs._streamName, this._streamName, lhsJoinFields, rhs.streamName(), rhsJoinFields, joinType, config));
  }
  
  
  private String streamName() {
    return this._streamName;
  }

  public StreamBuilder joinWith(StreamBuilder rhs, Fields lhsJoinFields, Fields rhsJoinFields) {
    return joinWith(rhs, lhsJoinFields, rhsJoinFields, JoinType.INNER, OperationConfig.createEmpty());
  }

  
  
  public StreamBuilder joinWith(StreamBuilder rhs, Fields joinFields) {
    return joinWith(rhs, joinFields, joinFields);
  }
  

  public StreamBuilder joinWith(StreamBuilder rhs, Fields fields, JoinType type) {
    return joinWith(rhs, fields, fields, type, OperationConfig.createEmpty());
  }

  

  public String name() {
    return this._streamName;
  }

  public Operation last() {
    return this._last;
  }
  
  public abstract StreamBuilder extend(String streamName);



  
}
