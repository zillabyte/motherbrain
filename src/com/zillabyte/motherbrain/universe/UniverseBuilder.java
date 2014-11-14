package com.zillabyte.motherbrain.universe;

import com.zillabyte.motherbrain.api.APIService;
import com.zillabyte.motherbrain.benchmarking.BenchmarkFactory;
import com.zillabyte.motherbrain.container.ContainerFactory;
import com.zillabyte.motherbrain.coordination.CoordinationService;
import com.zillabyte.motherbrain.coordination.CoordinationServiceWrapper;
import com.zillabyte.motherbrain.flow.FlowService;
import com.zillabyte.motherbrain.flow.aggregation.AggregationStoreFactory;
import com.zillabyte.motherbrain.flow.buffer.BufferClientFactory;
import com.zillabyte.motherbrain.flow.buffer.BufferService;
import com.zillabyte.motherbrain.flow.error.strategies.ErrorStrategyFactory;
import com.zillabyte.motherbrain.flow.operations.multilang.builder.FlowBuilderFactory;
import com.zillabyte.motherbrain.flow.rpc.queues.QueueFactory;
import com.zillabyte.motherbrain.metrics.Metrics;
import com.zillabyte.motherbrain.shell.LocalOsxShellFactory;
import com.zillabyte.motherbrain.shell.ShellFactory;
import com.zillabyte.motherbrain.shell.UbuntuEc2ShellFactory;
import com.zillabyte.motherbrain.shell.UbuntuTeamCityShellFactory;
import com.zillabyte.motherbrain.shell.UbuntuVagrantShellFactory;
import com.zillabyte.motherbrain.top.TopService;
import com.zillabyte.motherbrain.utils.Utils;
import com.zillabyte.motherbrain.utils.dfs.DFSService;

public class UniverseBuilder {

  private Universe _universe = new Universe();
  
  
  public UniverseBuilder withAPIService(APIService s) {
    _universe._api = s;
    return this;
  }
  
  public UniverseBuilder withErrorStrategy(ErrorStrategyFactory s) {
    _universe._errorStrategyFactory = s;
    return this;
  }
  
  public UniverseBuilder withSSHFactory(SSHFactory s) {
    _universe._sshFactory = s;
    return this;
  }
  
  public UniverseBuilder withFileFactory(FileFactory s) {
    _universe._fileFactory = s;
    return this;
  }
  
  public UniverseBuilder withLoggerFactory(LoggerFactory s) {
    _universe._loggerFactory = s;
    return this;
  }
  
  public UniverseBuilder withMetrics(Metrics s) {
    _universe._metrics = s;
    return this;
  }
  
  public UniverseBuilder withExceptionHandler(ExceptionHandler s) {
    _universe._exceptionHandler = s;
    return this;
  }

  public UniverseBuilder withQueueFactory(QueueFactory s) {
    _universe._queueFactory = s;
    return this;
  }
  
  public UniverseBuilder withBenchmarkFactory(BenchmarkFactory s) {
    _universe._benchmarkFactory = s;
    return this;
  }
  
  public UniverseBuilder addShellFactory() {
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
   
  public UniverseBuilder withDFSService(DFSService s) {
    _universe._dfsService = s;
    return this;
  }
  
  public UniverseBuilder withCoordinationService(CoordinationService s) {
    _universe._state = new CoordinationServiceWrapper(s);
    return this;
  }
  
  public UniverseBuilder withAggregationStoreFactory(AggregationStoreFactory s) {
    _universe._aggregationStoreFactory = s;
    return this;
  }
  
  public UniverseBuilder withFlowBuilderFactory(FlowBuilderFactory s) {
    _universe._flowBuilderFactory = s;
    return this;
  }
  
  public UniverseBuilder withContainerFactory(ContainerFactory s) {
    _universe._containerFactory = s;
    return this;
  }
  
  public UniverseBuilder withBufferClientFactory(BufferClientFactory s) {
    _universe._bufferClientFactory = s;
    return this;
  }
  
  public UniverseBuilder withBufferService(BufferService s) {
    _universe._bufferService = s;
    return this;
  }
  
  public UniverseBuilder withConfig(Config c) {
    _universe._config = c;
    return this;
  }
  
  public UniverseBuilder withConfig(String key, Object v) {
    _universe._config.put(key, v);
    return this;
  }
  
  public UniverseBuilder withEnvironment(Environment e) {
    _universe._env = e;
    return this;
  }
  
  public UniverseBuilder withTopService(TopService s) {
    _universe._topService = s;
    return this;
  }

  public UniverseBuilder withFlowService(FlowService s) {
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
  
  
  public static UniverseBuilder empty() {
    return new UniverseBuilder();
  }
  



}
