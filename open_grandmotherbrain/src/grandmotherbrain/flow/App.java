package grandmotherbrain.flow;

import grandmotherbrain.flow.components.ComponentOutput;
import grandmotherbrain.flow.config.FlowConfig;
import grandmotherbrain.flow.operations.Operation;
import grandmotherbrain.flow.operations.Sink;
import grandmotherbrain.flow.operations.Source;

import java.util.Collection;

import org.eclipse.jdt.annotation.NonNullByDefault;

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

  public @NonNullByDefault void connect(Operation source, Operation dest, String name) {
    this.graph().connect(source, dest, name);
  }

  public Integer getVersion() {
    return this.getFlowConfig().getFlowVersion();
  }
}
