package com.zillabyte.motherbrain.flow;

import java.util.Collection;

import org.eclipse.jdt.annotation.NonNullByDefault;

import com.zillabyte.motherbrain.flow.components.ComponentOutput;
import com.zillabyte.motherbrain.flow.config.FlowConfig;
import com.zillabyte.motherbrain.flow.operations.Operation;
import com.zillabyte.motherbrain.flow.operations.Sink;
import com.zillabyte.motherbrain.flow.operations.Source;

public class App extends Flow {

  private static final long serialVersionUID = -4123203117959924938L;
  
  public App(final String id, final String humanName) {
    super(id, humanName, FlowConfig.createEmpty());
  }
  
  public App(final String id, final String humanName, FlowConfig flowConfig) {
    super(id, humanName, flowConfig);
  }

  public App(FlowConfig config) {
    this(config.getFlowId(), config.getFlowId(), config);
  }

  public StreamBuilder.AppStreamBuilder createStream(Source s, String streamName) {
    return StreamBuilder.makeAppStreamBuilder(this, s, streamName);
  }

  public Collection<Source> sources() {
    return this.graph().getByType(Source.class);
  }

  public Collection<Sink> sinks() {
    return this.graph().getByType(Sink.class);
  }

  @Override
  public StreamBuilder createStream(ComponentOutput producer) {
    return StreamBuilder.makeAppStreamBuilder(this, producer, producer.produceStream());
  }

  public @NonNullByDefault void connect(Operation source, Operation dest, String name) throws FlowCompilationException{
    this.graph().connect(source, dest, name);
  }

  public Integer getVersion() {
    return this.getFlowConfig().getFlowVersion();
  }

  public void setVersion(Integer version) {
    _flowConfig.set("flow_version", version);
    
  }
}
