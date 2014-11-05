package com.zillabyte.motherbrain.universe;

import com.zillabyte.motherbrain.api.APIService;
import com.zillabyte.motherbrain.api.LocalAPIService;
import com.zillabyte.motherbrain.benchmarking.BenchmarkFactory;
import com.zillabyte.motherbrain.container.ContainerFactory;
import com.zillabyte.motherbrain.container.local.InplaceContainerFactory;
import com.zillabyte.motherbrain.coordination.CoordinationService;
import com.zillabyte.motherbrain.coordination.CoordinationServiceWrapper;
import com.zillabyte.motherbrain.coordination.mock.MockStateService;
import com.zillabyte.motherbrain.flow.FlowService;
import com.zillabyte.motherbrain.flow.aggregation.AggregationStoreFactory;
import com.zillabyte.motherbrain.flow.aggregation.DefaultAggregationStoreFactory;
import com.zillabyte.motherbrain.flow.buffer.BufferClientFactory;
import com.zillabyte.motherbrain.flow.buffer.BufferService;
import com.zillabyte.motherbrain.flow.buffer.mock.LocalBufferClientFactory;
import com.zillabyte.motherbrain.flow.buffer.mock.MockBufferService;
import com.zillabyte.motherbrain.flow.local.LocalFlowService;
import com.zillabyte.motherbrain.flow.operations.multilang.builder.FlowBuilderFactory;
import com.zillabyte.motherbrain.flow.operations.multilang.builder.InplaceFlowBuilderFactory;
import com.zillabyte.motherbrain.flow.rpc.queues.MockQueueFactory;
import com.zillabyte.motherbrain.flow.rpc.queues.QueueFactory;
import com.zillabyte.motherbrain.metrics.Metrics;
import com.zillabyte.motherbrain.metrics.MockMetrics;
import com.zillabyte.motherbrain.relational.RelationDefFactory;
import com.zillabyte.motherbrain.shell.LocalOsxShellFactory;
import com.zillabyte.motherbrain.shell.ShellFactory;
import com.zillabyte.motherbrain.shell.UbuntuEc2ShellFactory;
import com.zillabyte.motherbrain.shell.UbuntuTeamCityShellFactory;
import com.zillabyte.motherbrain.shell.UbuntuVagrantShellFactory;
import com.zillabyte.motherbrain.test.helpers.MockRelationDefFactory;
import com.zillabyte.motherbrain.top.BasicTopService;
import com.zillabyte.motherbrain.top.TopService;
import com.zillabyte.motherbrain.utils.Utils;
import com.zillabyte.motherbrain.utils.dfs.DFSService;
import com.zillabyte.motherbrain.utils.dfs.LocalDFSService;

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
