package grandmotherbrain.universe;

import grandmotherbrain.api.APIService;
import grandmotherbrain.api.LocalAPIService;
import grandmotherbrain.benchmarking.BenchmarkFactory;
import grandmotherbrain.container.ContainerFactory;
import grandmotherbrain.container.local.InplaceContainerFactory;
import grandmotherbrain.coordination.CoordinationService;
import grandmotherbrain.coordination.CoordinationServiceWrapper;
import grandmotherbrain.coordination.mock.MockStateService;
import grandmotherbrain.flow.FlowService;
import grandmotherbrain.flow.aggregation.AggregationStoreFactory;
import grandmotherbrain.flow.aggregation.DefaultAggregationStoreFactory;
import grandmotherbrain.flow.buffer.BufferClientFactory;
import grandmotherbrain.flow.buffer.BufferService;
import grandmotherbrain.flow.buffer.mock.LocalBufferClientFactory;
import grandmotherbrain.flow.buffer.mock.MockBufferService;
import grandmotherbrain.flow.local.LocalFlowService;
import grandmotherbrain.flow.operations.multilang.builder.FlowBuilderFactory;
import grandmotherbrain.flow.operations.multilang.builder.InplaceFlowBuilderFactory;
import grandmotherbrain.flow.rpc.queues.MockQueueFactory;
import grandmotherbrain.flow.rpc.queues.QueueFactory;
import grandmotherbrain.metrics.Metrics;
import grandmotherbrain.metrics.MockMetrics;
import grandmotherbrain.relational.RelationDefFactory;
import grandmotherbrain.shell.LocalOsxShellFactory;
import grandmotherbrain.shell.ShellFactory;
import grandmotherbrain.shell.UbuntuEc2ShellFactory;
import grandmotherbrain.shell.UbuntuTeamCityShellFactory;
import grandmotherbrain.shell.UbuntuVagrantShellFactory;
import grandmotherbrain.test.helpers.MockRelationDefFactory;
import grandmotherbrain.top.BasicTopService;
import grandmotherbrain.top.TopService;
import grandmotherbrain.utils.Utils;
import grandmotherbrain.utils.dfs.DFSService;
import grandmotherbrain.utils.dfs.LocalDFSService;

public class LocalUniverseBuilder {

  private Universe _universe = new Universe();
  
  
  public LocalUniverseBuilder withAPIService(APIService s) {
    _universe._api = s;
    return this;
  }
  
  public LocalUniverseBuilder withRelationFactory(RelationDefFactory s) {
    _universe._relFactory = s;
    return this;
  }
  
  public LocalUniverseBuilder withSSHFactory(SSHFactory s) {
    _universe._sshFactory = s;
    return this;
  }
  
  public LocalUniverseBuilder withFileFactory(FileFactory s) {
    _universe._fileFactory = s;
    return this;
  }
  
  public LocalUniverseBuilder withLoggerFactory(LoggerFactory s) {
    _universe._loggerFactory = s;
    return this;
  }
  
  public LocalUniverseBuilder withMetrics(Metrics s) {
    _universe._metrics = s;
    return this;
  }
  
  public LocalUniverseBuilder withExceptionHandler(ExceptionHandler s) {
    _universe._exceptionHandler = s;
    return this;
  }

  public LocalUniverseBuilder withQueueFactory(QueueFactory s) {
    _universe._queueFactory = s;
    return this;
  }
  
  public LocalUniverseBuilder withBenchmarkFactory(BenchmarkFactory s) {
    _universe._benchmarkFactory = s;
    return this;
  }
  
  public LocalUniverseBuilder addShellFactory() {
    ShellFactory shellFactory;
    switch(Utils.getMachineType()) {
    case UBUNTU_VAGRANT:
      shellFactory = new UbuntuVagrantShellFactory();
      break;
    case UBUNTU_EC2:
      shellFactory = new UbuntuEc2ShellFactory();
      break;
    case OSX_LOCAL:
      shellFactory = new LocalOsxShellFactory();
      break;
    case UBUNTU_TEAMCITY:
      shellFactory = new UbuntuTeamCityShellFactory();
      break;
    default:
      throw new RuntimeException("Unknown OS type");
    }
    _universe._shellFactory = shellFactory;
    return this;
  }
   
  public LocalUniverseBuilder withDFSService(DFSService s) {
    _universe._dfsService = s;
    return this;
  }
  
  public LocalUniverseBuilder withCoordinationService(CoordinationService s) {
    _universe._state = new CoordinationServiceWrapper(s);
    return this;
  }
  
  public LocalUniverseBuilder withAggregationStoreFactory(AggregationStoreFactory s) {
    _universe._aggregationStoreFactory = s;
    return this;
  }
  
  public LocalUniverseBuilder withFlowBuilderFactory(FlowBuilderFactory s) {
    _universe._flowBuilderFactory = s;
    return this;
  }
  
  public LocalUniverseBuilder withContainerFactory(ContainerFactory s) {
    _universe._containerFactory = s;
    return this;
  }
  
  public LocalUniverseBuilder withBufferClientFactory(BufferClientFactory s) {
    _universe._bufferClientFactory = s;
    return this;
  }
  
  public LocalUniverseBuilder withBufferService(BufferService s) {
    _universe._bufferService = s;
    return this;
  }
  
  public LocalUniverseBuilder withConfig(Config c) {
    _universe._config = c;
    return this;
  }
  
  public LocalUniverseBuilder withConfig(String key, Object v) {
    _universe._config.put(key, v);
    return this;
  }
  
  public LocalUniverseBuilder withEnvironment(Environment e) {
    _universe._env = e;
    return this;
  }
  
  public LocalUniverseBuilder withTopService(TopService s) {
    _universe._topService = s;
    return this;
  }

  public LocalUniverseBuilder withFlowService(FlowService s) {
    _universe._flowService = s;
    return this;
  }
  
  
  
  public Universe create() {
    if (Universe.hasInstance()) {
      throw new IllegalStateException("A universe has already been created!");
    }
    Universe.setInstance(_universe);
    return _universe;
  }
  
  public Universe forceCreate() {
    Universe.setInstance(_universe);
    return _universe;
  }
  
  
  public static LocalUniverseBuilder empty() {
    return new LocalUniverseBuilder();
  }
  
  
  /***
   * The default 'local' universe is meant for running zb local.
   * @param config
   * @return
   */
  public static LocalUniverseBuilder buildDefaultLocal(Config config) {
    
    config.put("transaction.success.timeout", Long.valueOf(1000 * 30));
    config.put("operation.idle.trigger.period", Long.valueOf(1000));
    config.put("state.streams.emit_permission.timeout", 500L);

    return empty()
        .withEnvironment(Environment.local())
        .withConfig(config)
        .withFlowBuilderFactory(new InplaceFlowBuilderFactory())
        .withRelationFactory(new MockRelationDefFactory())
        .withTopService(new BasicTopService())
        .withFlowService(new LocalFlowService())
        .withDFSService(new LocalDFSService())
        .withMetrics(new MockMetrics())
        .withAPIService(new LocalAPIService())
        .withCoordinationService(new MockStateService())
        .withSSHFactory(new SSHFactory.Local())
        .withFileFactory(new FileFactory.Local())
        .withContainerFactory(new InplaceContainerFactory(Utils.expandPath(config.get("directory", System.getProperty("user.dir")))))
        .withLoggerFactory(new LoggerFactory.Mock())
        .withExceptionHandler(new ExceptionHandler())
        .withQueueFactory(new MockQueueFactory())
        .withAggregationStoreFactory(new DefaultAggregationStoreFactory())
        .withBufferClientFactory(new LocalBufferClientFactory())
        .withBufferService(new MockBufferService())
        .addShellFactory();
  }
  
  public static Universe createDefaultLocal(Config c) {
    return buildDefaultLocal(c).forceCreate();
  }

}
