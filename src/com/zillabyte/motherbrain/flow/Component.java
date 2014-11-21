package com.zillabyte.motherbrain.flow;

import java.util.Collection;

import org.eclipse.jdt.annotation.NonNullByDefault;

import com.zillabyte.motherbrain.flow.components.ComponentInput;
import com.zillabyte.motherbrain.flow.components.ComponentOutput;
import com.zillabyte.motherbrain.flow.config.FlowConfig;
import com.zillabyte.motherbrain.flow.graph.FlowGraph;

@NonNullByDefault
public class Component extends Flow {

  private boolean _shouldMerge = false;
  private static final long serialVersionUID = -7661318466835721741L;


  public Component(final String id, FlowConfig flowConfig) {
    this(id, id, flowConfig);
  }
  
  public Component(final String id) {
    this(id, id);
  }
  
  public Component(final String id, final String humanName, FlowConfig flowConfig) {
    super(id, humanName, flowConfig);
  }
  
  public Component(final String id, final String humanName) {
    super(id, humanName, FlowConfig.createEmpty());
  }
  
  public StreamBuilder.ComponentStreamBuilder createStream(ComponentInput s, String streamName) {
    return StreamBuilder.makeComponentStreamBuilder(this, s, streamName);
  }


  public Collection<ComponentInput> inputs() {
    return this.graph().getByTypeAndContainer(ComponentInput.class, this);
  }

  public ComponentInput getOneInput() {
    if (inputs().size() != 1) 
      throw new IllegalStateException("only one input was expected, instead: " + inputs());
    return inputs().iterator().next();
  }
  
  public Collection<ComponentOutput> outputs() {
    return this.graph().getByTypeAndContainer(ComponentOutput.class, this);
  }
  
  public ComponentOutput getOneOutput() {
    if (outputs().size() != 1) throw new IllegalStateException("only one output was expected");
    return outputs().iterator().next();
  }

  @Override
  public StreamBuilder createStream(ComponentOutput producer) {
    return StreamBuilder.makeComponentStreamBuilder(this, producer, producer.produceStream());
  }
  
  
  public void setMerge(boolean b) {
    _shouldMerge = b;
  }
  
  public boolean shouldMerge() {
    return _shouldMerge;
  }

  public void setGraph(FlowGraph flowGraph) {
    _graph = flowGraph;
  }
  
}
