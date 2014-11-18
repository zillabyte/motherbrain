package com.zillabyte.motherbrain.universe;

import java.io.Serializable;

import org.apache.log4j.Logger;

import com.zillabyte.motherbrain.api.APIService;
import com.zillabyte.motherbrain.benchmarking.BenchmarkFactory;
import com.zillabyte.motherbrain.container.ContainerFactory;
import com.zillabyte.motherbrain.coordination.CoordinationServiceWrapper;
import com.zillabyte.motherbrain.flow.FlowService;
import com.zillabyte.motherbrain.flow.aggregation.AggregationStoreFactory;
import com.zillabyte.motherbrain.flow.buffer.BufferClientFactory;
import com.zillabyte.motherbrain.flow.buffer.BufferService;
import com.zillabyte.motherbrain.flow.error.strategies.ErrorStrategyFactory;
import com.zillabyte.motherbrain.flow.operations.multilang.builder.FlowBuilderFactory;
import com.zillabyte.motherbrain.flow.rpc.queues.QueueFactory;
import com.zillabyte.motherbrain.metrics.Metrics;
import com.zillabyte.motherbrain.shell.ShellFactory;
import com.zillabyte.motherbrain.top.TopService;
import com.zillabyte.motherbrain.utils.dfs.DFSService;
import com.zillabyte.motherbrain.utils.dfs.DFSServiceWrapper;


/****
 * The universe object is a singleton (i.e. it only has one instance in the entire JVM).  It is meant to 
 * capture the state of the entire GrandmotherBrain system.  I.e. it should be used to track which Flows 
 * are running, their underlying buffers, provisions. etc.  The goal is to be able to terminate the 
 * entire system, then restart in the same state.  
 *
 */
public class Universe implements Serializable {

  private static final long serialVersionUID = -1384275399314055348L;
  private static final Logger log = Logger.getLogger(Universe.class);
  
  // Master Services  
  transient FlowService _flowService = null;
  transient TopService _topService = null;
  
  // Support Services/Factories

  protected  APIService _api = null;
  protected  SSHFactory _sshFactory = null;
  protected  FileFactory _fileFactory = null;
  protected  LoggerFactory _loggerFactory = null;
  protected  Metrics _metrics = null;
  protected  ExceptionHandler _exceptionHandler = null;
  protected  QueueFactory _queueFactory = null;
  protected  BenchmarkFactory _benchmarkFactory = new BenchmarkFactory.Noop();
  protected  ShellFactory _shellFactory = null; 
  protected  DFSService _dfsService = null;
  protected  CoordinationServiceWrapper _state = null;
  protected  AggregationStoreFactory _aggregationStoreFactory = null;  
  protected  FlowBuilderFactory _flowBuilderFactory = null;
  protected  ContainerFactory _containerFactory = null;
  protected  BufferClientFactory _bufferClientFactory = null;
  protected  BufferService _bufferService = null;
  protected  ErrorStrategyFactory _errorStrategyFactory = null;
  
  
  // Misc 
  Config _config = new Config();
  Environment _env = null;
  
  
  // Singleton 
  private static Universe _instance = null;
  
  
  // Package-private constructor.  Only UniverseBuilder should build a universe.
  public Universe() {
  }
  

  
  public SSHFactory sshFactory() {
    return this._sshFactory;
  }
  
  public FileFactory fileFactory() {
    return this._fileFactory;
  }
  
  public FlowService flowService() {
    return this._flowService;
  }



  /**
   * 
   */
  public TopService topService() {
    return this._topService;
  }
  
  
  
  public LoggerFactory loggerFactory() {
    return this._loggerFactory;
  }
  
  public Metrics metrics() {
    return this._metrics;
  }

  public BufferClientFactory bufferClientFactory(){
    return this._bufferClientFactory;
  }
  
  public BufferService bufferService(){
    return this._bufferService;
  }
  
  public DFSServiceWrapper dfsService() {
    return new DFSServiceWrapper(this._dfsService);
  }

  
  public Environment env() {
    return this._env;
  }

  
  public CoordinationServiceWrapper state() {
    return _state;
  }



  public APIService api() {
    return _api;
  }



  
  
  public Config config() {
    return _config;
  }




  public AggregationStoreFactory aggregationStoreFactory() {
    return _aggregationStoreFactory;
  }



  public QueueFactory rpcQueueFactory() {
    return _queueFactory;
  }
  
  
  public ErrorStrategyFactory errorStrategyFactory() { 
    return _errorStrategyFactory;
  }


  public BenchmarkFactory benchmarkFactory() {
    return this._benchmarkFactory;
  }

  
  public ContainerFactory containerFactory() {
    return _containerFactory;
  }
  
  public FlowBuilderFactory flowBuilderFactory() {
    return _flowBuilderFactory;
  }
    

  public ShellFactory shellFactory() {
    return this._shellFactory;
  }

  public static Universe instance() {
    if (_instance == null) throw new IllegalStateException("Universe has not been created!");
    return _instance;
  }

  static synchronized void setInstance(Universe uni) {
    _instance = uni;
  }


  public synchronized static void maybeCreate(Universe uni) {
    if (_instance == null) {
      if (uni == null) {
        throw new NullPointerException("given universe is null");
      }
      log.info("Initializing state machine...");
      setInstance(uni);
      log.info("Universe successfully instantiated.");
    }
  }

  
  public synchronized static boolean hasInstance() {
    return _instance != null;
  }
  
}
