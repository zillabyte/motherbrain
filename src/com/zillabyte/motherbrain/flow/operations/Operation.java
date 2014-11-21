package com.zillabyte.motherbrain.flow.operations;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import net.sf.json.JSONObject;

import org.apache.commons.lang.SerializationUtils;
import org.codehaus.plexus.util.ExceptionUtils;
import org.eclipse.jdt.annotation.NonNullByDefault;
import org.javatuples.Triplet;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.monitoring.runtime.instrumentation.common.com.google.common.collect.LinkedListMultimap;
import com.zillabyte.motherbrain.benchmarking.Benchmark;
import com.zillabyte.motherbrain.coordination.CoordinationException;
import com.zillabyte.motherbrain.coordination.Lock;
import com.zillabyte.motherbrain.coordination.MessageHandler;
import com.zillabyte.motherbrain.coordination.Watcher;
import com.zillabyte.motherbrain.flow.Fields;
import com.zillabyte.motherbrain.flow.Flow;
import com.zillabyte.motherbrain.flow.StateMachineException;
import com.zillabyte.motherbrain.flow.collectors.OutputCollector;
import com.zillabyte.motherbrain.flow.collectors.coordinated.CoordinatedOutputCollector;
import com.zillabyte.motherbrain.flow.config.OperationConfig;
import com.zillabyte.motherbrain.flow.config.UserConfig;
import com.zillabyte.motherbrain.flow.error.strategies.FakeLocalException;
import com.zillabyte.motherbrain.flow.error.strategies.OperationErrorStrategy;
import com.zillabyte.motherbrain.flow.graph.Connection;
import com.zillabyte.motherbrain.flow.heartbeats.Heartbeat;
import com.zillabyte.motherbrain.flow.heartbeats.HeartbeatException;
import com.zillabyte.motherbrain.flow.operations.decorators.EmitDecorator;
import com.zillabyte.motherbrain.flow.operations.multilang.MultiLangException;
import com.zillabyte.motherbrain.metrics.Metrics;
import com.zillabyte.motherbrain.relational.DefaultStreamException;
import com.zillabyte.motherbrain.top.MotherbrainException;
import com.zillabyte.motherbrain.universe.Config;
import com.zillabyte.motherbrain.universe.S3Exception;
import com.zillabyte.motherbrain.universe.Universe;
import com.zillabyte.motherbrain.utils.DateHelper;
import com.zillabyte.motherbrain.utils.JSONUtil;
import com.zillabyte.motherbrain.utils.Log4jWrapper;
import com.zillabyte.motherbrain.utils.SerializableMonitor;
import com.zillabyte.motherbrain.utils.Utils;
import com.zillabyte.motherbrain.utils.backoff.ExponentialBackoffTicker;

public abstract class Operation implements Serializable {

  private static final long serialVersionUID = -7294936970361954239L;

  public static final long IDLE_TRIGGER_PERIOD_DEFAULT = 1000L * 10;
  public static final long ACTIVE_OPERATION_TIMEOUT_DEFAULT = 1000L * 60 * 15;
  public static final String COMPONENT_CARRY_FIELD_PREFIX = "_CARRY_FIELD_";
  public static final HashSet<String> NONLINEAR_OPS = Sets.newHashSet("group_by", "join");
  public static final String SOURCE_STATE_KEY_PREFIX = "/__state/";
  public static final Integer DEFAULT_MAX_PARALLELISM = 20;
  public static final long HEARTBEAT_DEATH_SLEEP = 1000L;

  private Log4jWrapper _log = Log4jWrapper.create(Operation.class, this);
  private long _lastActivity = System.currentTimeMillis();
  private final String _userGivenName;
  private String _namespacePrefix = "";
  private Integer _instanceIndex = null;
  private AtomicInteger _inOperationCounter = new AtomicInteger();
  private int _maxParallelism = 20;
  private int _targetParallelism = 1;
  private boolean _parallelismOverriden = false;
  protected Flow _containerFlow = null;
  private Map<String, String> _extraInfo;
  private int _emitCount;
  private int _loopCalls;
  private int _consumedCount;
  private OperationConfig _operationConfig;
  private Long _idleTriggerPeriod;
  protected Long _activityTimeout;
  private Long _transactional_msg_timeout;
  private Long _initial_stage_timeout_ms;
  private Long _pre_prepare_stage_timeout_ms;
  private Long _prepare_stage_timeout_ms;
  
  protected transient ExecutorService _executor = null;

  private SerializableMonitor _messageMonitor = new SerializableMonitor();

  protected LinkedListMultimap<String, EmitDecorator> _emitDecorators = LinkedListMultimap.create();
  protected boolean _mergeIOFields = false;
  protected OperationLogger _operationLogger = new OperationLogger.noOp();
  protected ExponentialBackoffTicker _ipcLogBackoff = new ExponentialBackoffTicker(
      10000/* 100 */);
  protected transient Heartbeat _heartbeat = null;
  protected transient OperationErrorStrategy _errorStrategy = null;
  protected int _loopErrors = 0;
  protected OperationSleeper _sleeper = new OperationSleeper();
  protected Map<String, Fields> _expectedFields = new HashMap<>();
  protected transient OutputCollector _collector;
  private Fields _incomingRouteByFields = null;

  private Integer _actualParallelism = null;
  
  private Set<String> _opsFromSelfToSelf;
  private Set<String> _opsFromSourcesToSelf;
  private Set<String> _opsFromSelfToSinks;
  private Set<String> _adjacentUpStreamNonLoopOps;
  private Set<String> _adjacentDownStreamOps;

  private Watcher _flowCommandWatcher = null;

  

  /***
   * 
   * @param name
   */
  public Operation(String name, OperationConfig opConfig) {
    _userGivenName = name;
    _operationConfig = opConfig;

    _idleTriggerPeriod = Config.getOrDefault("operation.idle.trigger.period", IDLE_TRIGGER_PERIOD_DEFAULT).longValue();
    _transactional_msg_timeout = Config.getOrDefault("operation.state.transaction.timeout", 1000 * 30L);
    _initial_stage_timeout_ms = Config.getOrDefault("operation.initial.stage.timeout", 1000 * 60L * 2);
    _pre_prepare_stage_timeout_ms = Config.getOrDefault("operation.pre_prepare.stage.timeout", 1000 * 60L * 2);
    _prepare_stage_timeout_ms = Config.getOrDefault("operation.prepare.stage.timeout", 1000 * 60 * 10L);
    _extraInfo = Maps.newHashMap();

    // Overrides?
    _activityTimeout = Config.getOrDefault("operation.activity.timeout", ACTIVE_OPERATION_TIMEOUT_DEFAULT).longValue();
    if (opConfig.containsKey("timeout")) {
      Long period = DateHelper.parseDuration(opConfig.get("timeout", ""));
      if (period != null && period > 0) {
        _activityTimeout = period;
        _log.info("using custom timeout of: " + _activityTimeout);
      }
    }
    
    // Apply run related snapshots
    applySnapshotIfExists_ThreadUnsafe();
    
    
  }


  public Operation(String name) {
    this(name, OperationConfig.createEmpty());
  }

  public void parseFlowGraph() {
    _opsFromSelfToSelf = Sets.newHashSet();
    _opsFromSourcesToSelf = Sets.newHashSet();
    _opsFromSelfToSinks = Sets.newHashSet();
    _adjacentDownStreamOps = Sets.newHashSet();
    _adjacentUpStreamNonLoopOps = Sets.newHashSet();

    if (!type().equalsIgnoreCase("source")) {
      _opsFromSelfToSelf = getTopFlow().graph().operationsBetween(this, this);
      for (Operation o : getTopFlow().graph().sources()) {
        _opsFromSourcesToSelf.addAll(getTopFlow().graph().operationsBetween(o,
            this));
      }
    }
    if (!type().equalsIgnoreCase("sink")) {
      for (Operation o : getTopFlow().graph().sinks()) {
        _opsFromSelfToSinks.addAll(getTopFlow().graph().operationsBetween(this,
            o));
      }
      _opsFromSelfToSinks.remove(this.namespaceName()); // exclude self
    }

    for(Connection c : getTopFlow().graph().connectionsFrom(this)) {
      _adjacentDownStreamOps.add(c.dest().namespaceName());
    }
    for (Connection c : getTopFlow().graph().connectionsTo(this)) {
      if (!c.loopBack())
        _adjacentUpStreamNonLoopOps.add(c.source().namespaceName());
    }
  }

  public Set<String> opsFromSourcesToSelf() {
    return _opsFromSourcesToSelf;
  }

  public Set<String> opsFromSelfToSelf() {
    return _opsFromSelfToSelf;
  }

  public Set<String> opsFromSelfToSinks() {
    return _opsFromSelfToSinks;
  }

  public Set<String> adjacentUpStreamNonLoopOps() {
    return _adjacentUpStreamNonLoopOps;
  }

  public Set<String> adjacentDownStreamOps() {
    return _adjacentDownStreamOps;
  }

  /**
   * 
   */
  public final void handleStats_ThreadUnsafe() throws InterruptedException,
      CoordinationException {

    // Init
    Map<String, Object> stats = Maps.newHashMap();
    if (_heartbeat != null)
      stats.put("last_heartbeat", _heartbeat.getLastHeartbeat());

    // Stats...
    stats.put("last_activity", Long.toString(_lastActivity));
    stats.put("errors", Long.toString(getErrorCount()));
    if (_collector != null) {
      stats.put("consumed", Long.toString(_collector.getConsumeCount()));
      stats.put("emitted", Long.toString(_collector.getEmitCount()));
      stats.put("parallelism", Integer.toString(getTargetParallelism()));
      stats.put("acks", Long.toString(_collector.getAckCount()));
      stats.put("fails", Long.toString(_collector.getFailCount()));
      stats.put("coord_emits", Long.toString(_collector.getCoordEmitCount()));
      stats.put("coord_consumed",
          Long.toString(_collector.getCoordConsumeCount()));

      if (_collector instanceof CoordinatedOutputCollector) {
        CoordinatedOutputCollector asCoord = (CoordinatedOutputCollector)_collector;
        Triplet<Integer, Integer, Integer> triplet = asCoord.getUnackedAndLocalQueueAndRemoteQueeuCount_ThreadUnsafe();
        stats.put("unacked", Integer.toString(triplet.getValue0()));
        stats.put("queued_local", Integer.toString(triplet.getValue1()));
        stats.put("queued_remote", Integer.toString(triplet.getValue2()));
        stats.put("pressure_state", asCoord.inPressureState());
      }

    }

    // Other (user defined) stats...
    updateMiscStats(stats);

    // Done
    this.sendMessageToFlow_ThreadUnsafe("stats", stats);
  }

  public void handleCoordination_ThreadUnsafe() {
    if (this._collector != null) {
      this._collector.handleChecks();
    }
  }

  /**
   * 
   */
  public void handleActivityCheck_ThreadUnsafe() throws InterruptedException, OperationException {
    // _log.debug(instanceName()+" -- In handle activity check: "+_lastActivity+" "+_inOperation);
    if (_lastActivity + this._idleTriggerPeriod < System.currentTimeMillis() && _inOperationCounter.get() == 0) {
      handleIdleDetected();
    }

  }

  public final String operationId() {
    return userGivenName();
  }

  public final Integer instanceIndex() {
    return _instanceIndex;
  }

  public final String instanceName() {
    return namespaceName() + "." + instanceIndex();
  }

  public final String userGivenName() {
    return _userGivenName;
  }

  public final String namespaceName() {
    if (_namespacePrefix.equals("")) {
      return _userGivenName;
    }
    return _namespacePrefix + "." + _userGivenName;
  }
  
  public final String namespaceOperationId() {
    if (_namespacePrefix.equals("")) {
      return operationId();
    }
    return _namespacePrefix + "." + operationId();
  }

  /****
   * Retrhows any error that have been caught be other threads..
   */
  public void heartbeatErrorCheck_ThreadUnsafe() throws InterruptedException,
      OperationException, FakeLocalException {

    // Init
    Throwable rethrow = null;

    // Heartbeat error?
    rethrow = _heartbeat.maybeGetHeartbeatException();
    if (rethrow != null) {
      _errorStrategy.handleHeartbeatDeath();
      // throw new OperationException(this, rethrow);
    }

    // Is heartbeat even running?
    if (_heartbeat.isRunning() == false) {
      _errorStrategy.handleHeartbeatDeath();
      // throw new OperationException("heartbeat is not running");
    }

    // Do we have any fatal errors?
    rethrow = _errorStrategy.maybeGetFatalError();
    if (rethrow != null) {
      throw new OperationException(this, rethrow);
    }
  }

  protected final static Metrics metrics() {
    return Universe.instance().metrics();
  }

  /**
   * Override me!
   * @throws OperationException 
   */
  public void prepare() throws MultiLangException, InterruptedException, OperationException {
  }

  /**
   * Override me!
   */
  public void prePrepare() throws InterruptedException, OperationException {
  }

  /**
   * Override me!
   */
  public void postPrepare() throws InterruptedException, OperationException {

  }

  public void addEmitDecorator(String stream, EmitDecorator dec) {
    _emitDecorators.put(stream, dec);
    dec.setOperation(this);
  }

  public Collection<EmitDecorator> emitDecorators(String stream) {
    return _emitDecorators.get(stream);
  }

  public boolean inPressureState() {
    // This is our pressure valve. We sometimes need to let Storm catch up and
    // process all
    // the tuples we're emitting. To do this, we allow for up to N unacked
    // tuples at a time.
    // once that threshold has been hit, we wait for all tuples to ack (or fail)
    // before
    // proceeding.
    return _collector.inPressureState();
  }

  public OperationLogger logger() {
    return _operationLogger;
  }
  

  public Heartbeat getHeartbeat() {
    return _heartbeat;
  }

  public void incLoop() {
    _loopCalls++;
  }

  public void incLoop(long l) {
    _loopCalls += l;
  }

  public long getLoopCalls() {
    return _loopCalls;
  }

  /***
   * Returns a unique string suitable for using with global locks. Note that
   * this uses the Flow#run_id, which helps us avoid the situation where
   * previous locks of a flow die or otherwise don't get unlocked.
   * 
   * @return
   */
  protected String lockPrefix() {
    return topFlowId() + "/cycle_" + getTopFlow().getVersion();
  }

  protected String workerHost() {
    return Utils.getHost();
  }

  protected long incEmit(long l) {
    return _emitCount += l;
  }

  protected long incEmit() {
    return _emitCount++;
  }

  protected long getEmitCount() {
    return _emitCount;
  }

  protected long incConsumed() {
    return _consumedCount++;
  }

  protected long getConsumedCount() {
    return _consumedCount;
  }

  protected long getErrorCount() {
    return this._errorStrategy.getErrorCount();
  }

  protected OperationErrorStrategy getErrorStrategy() {
    return this._errorStrategy;
  }

  protected Operation setConfig(OperationConfig config) {
    this._operationConfig = config;
    return this;
  }

  protected OperationConfig getConfig() {
    return this._operationConfig;
  }

  public OutputCollector getOutputCollector() {
    return _collector;
  }

  public Map<String, String> extraInfo() {
    return _extraInfo;
  }
  
  public void addExtraInfo(String key, String value){
    _extraInfo.put(key, value);
  }

  public OperationSleeper getSleeper() {
    return _sleeper;
  }

  public final void setExtraInfo(Map<String, String> extraInfo) {
    _extraInfo = extraInfo;
  }

  @Override
  public Operation clone() {
    Operation op = (Operation) SerializationUtils.clone(this);
    op._expectedFields.clear();
    op._containerFlow = null;
    return op;
  }

  public void setContainerFlow(Flow f) {
    this._containerFlow = f;
  }


  protected void reportInfo() throws OperationException, InterruptedException {

    Map<String, Object> basicInfo = Maps.newHashMap();
    if (_extraInfo != null)
      basicInfo.putAll(_extraInfo);
    final String host = workerHost();
    if (host != null) {
      basicInfo.put("host", host);
    }
    final Integer instanceIndex = this.instanceIndex();
    if (instanceIndex != null) {
      basicInfo.put("instance_index", this.instanceIndex());
    }
    basicInfo.put("instance_name", this.instanceName());
    basicInfo.put("type", this.type());
    basicInfo.put("name", this.namespaceName());
    basicInfo.put("operation_id", this.namespaceOperationId());
    try {
      sendTransactionalMessageToFlow("info", basicInfo);
    } catch (TimeoutException | CoordinationException e) {
      throw new OperationException(this, e);
    }

  }


  protected void errorCheck() {
    // Init
    Throwable rethrow = null;

    try {
      // Do we have unhandled heartbeat exceptions?
      if (_heartbeat.maybeGetHeartbeatException() != null) {
        _log.info("Error check picked up heartbeat error");
        getErrorStrategy().handleHeartbeatDeath();
      }

      // Do we have any fatal errors?
      rethrow = getErrorStrategy().maybeGetFatalError();
      if (rethrow != null) {
        throw new OperationException(this, rethrow);
      }
    } catch (OperationException | FakeLocalException e) {
      _log.warn("Error occurred during error check in " + instanceName() + ": "
          + e.getMessage());
    }

  }

  protected void notifyOfNewState(String newState, boolean transactional) throws TimeoutException, CoordinationException {
    if (transactional) {
      sendTransactionalMessageToFlow("state", newState);
    } else {
      sendMessageToFlow_ThreadUnsafe("state", newState);
    }
  }

  /***
   * 
   */
  public final void markBeginActivity() {
    // _log.debug(instanceName() + " markBeginActivity: " +
    // _inOperationCounter);
    _lastActivity = System.currentTimeMillis();
    _inOperationCounter.incrementAndGet();
  }

  /***
   * 
   */
  public final void markEndActivity() {
    // _log.debug(instanceName() + " markEndActivity: " + _inOperationCounter);
    _lastActivity = System.currentTimeMillis();
    if (_inOperationCounter.decrementAndGet() < 0)
      throw new IllegalStateException(
          "a markEndActivity() looks like it was called without a corresponding markBeginActivity()");
  }

  /**
   * Override me!
   */
  public void cleanup() throws MultiLangException, OperationException,
      InterruptedException {
    /* noop */
  }

  /***
   * REACTOR SAFE
   * 
   * @throws TimeoutException
   * @throws CoordinationException
   * @throws StateMachineException
   */
  public synchronized final void handleCleanup()  {

    // Sanity...
    if (inState("KILLING", "KILLED"))
      return;

    // INIT
    _log.info(instanceName() + " cleaning up: " + this.toString());
    if (!inState("ERROR", "ERRORING")) {
      try {
        transitionToState("KILLING");
      } catch (StateMachineException | CoordinationException | TimeoutException e) {
        e.printStackTrace();
      }
    }

    // Stop listeners... 
    try {
      stopWatchingFlowCommands();
    } catch (CoordinationException e) {
      e.printStackTrace();
    }
    
    try {
      cleanup();
    } catch (OperationException | InterruptedException e) {
      e.printStackTrace();
    }
    
    if (_heartbeat != null)
      _heartbeat.shutdown();

    // Done
    if (!inState("ERROR", "ERRORING")) {
      try {
        transitionToState("KILLED");
      } catch (StateMachineException | CoordinationException | TimeoutException e) {
        e.printStackTrace();
      }
    }

    // Stop threads related to this operaiton...
    if (_executor != null) {
      _log.info("stopping executor...");
      _executor.shutdownNow();
    }
  }

  /***
   * Sends a message to the flowinstance. REACTOR SAFE
   */
  public void sendMessageToFlow_ThreadUnsafe(final String command,
      final Object payload) throws CoordinationException {
    synchronized (_messageMonitor) {
      Universe
          .instance()
          .state()
          .sendMessage(flowStateKey(),
              OperationMessage.create(Operation.this, command, payload));
    }
  }

  protected void sendTransactionalMessageToFlow(String command, Object payload)
      throws TimeoutException, CoordinationException {
    synchronized (_messageMonitor) {
      Universe
          .instance()
          .state()
          .sendTransactionalMessage(_executor, this.flowStateKey(),
              OperationMessage.create(this, command, payload),
              _transactional_msg_timeout);
    }
  }

  protected void updateMiscStats(Map<String, Object> stats) {
  }

  protected synchronized void handleIdleDetected() throws InterruptedException, OperationException {
  }

  public void handlePostHeartbeat_ThreadUnsafe() {
    // called after a heartbeat. use for testing.
  }

  public void onSetExpectedFields() throws OperationException {
    if (_incomingRouteByFields != null) {
      for (Connection conn : this.prevConnections()) {
        for (String field : _incomingRouteByFields) {
          conn.source().addExpectedFields(conn.streamName(), new Fields(field));
        }
      }
    }
  }

  public void onThisBatchCompleted(final Object batchId) {

    // Shall we 'fast-idle'? We only do this for non-rpcs that have no
    // loopbacks. In the case of
    // an RPC, the operation state should only idle when there truly hasn't been
    // any activity. The
    // no-loopback condition is a bit of a hack for now because loopback'ed
    // flows don't really know
    // when they're fully done processing. TODO: remove this condition if/when
    // we figure out how
    // to make loopback-flows signal total completion of a batch.
    if (!this.getTopFlow().getFlowConfig().get("rpc", false)
        && this.getTopFlow().graph().hasLoopbacks() == false) {
      try {
        synchronized (this) {
          handleIdleDetected();
          if (this.inState("EMITTING", "ACTIVE"))
            transitionToState("IDLE");
        }
      } catch (StateMachineException | CoordinationException | TimeoutException
          | InterruptedException | OperationException e) {
        // Not the end of the world if this errors.... Do nothing.
        _log.error("error in preemptive idle: "
            + ExceptionUtils.getFullStackTrace(e));
      }
    }
  }

  public String flowStateKey() {
    return "flows/" + this.topFlowId() +  "/cycle_" + getTopFlow().getVersion();
  }

  public String operationStateKey() {
    return flowStateKey() + "/operations/" + this.namespaceName();
  }

  public String instanceStateKey() {
    return operationStateKey() + "/instances/" + instanceName();
  }

  public String topFlowId() {
    final Flow flow = getTopFlow();
    if (flow == null) {
      throw new NullPointerException("flow has not been set! " + this.toString());
    }
    return flow.getId();
  }

  public Flow getTopFlow() {
    if (this._containerFlow == null) {
      throw new NullPointerException("setContainerFlow(..) has not been called! " + this.instanceName());
    }
    return _containerFlow.getTopFlow();
  }

  public Flow getContainerFlow() {
    return this._containerFlow;
  }

  public abstract String type();


  public void handleFatalError(Throwable e) throws OperationException, FakeLocalException {
    if(e instanceof MotherbrainException) {
      _operationLogger.writeLog("FATAL ERROR:", OperationLogger.LogPriority.ERROR);
      _operationLogger.logError((Exception) e);
    }
    this._errorStrategy.handleFatalError(e);
  }


  public void handleLoopError(Throwable e) throws OperationException, FakeLocalException {
    if(e instanceof MotherbrainException) {
      _operationLogger.writeLog("LOOP ERROR/WARNING:", OperationLogger.LogPriority.ERROR);
      _operationLogger.logError((Exception) e);
    }
    this._errorStrategy.handleLoopError(e);
  }

  public void reportError(final Throwable e) throws InterruptedException,
      CoordinationException {
    this.sendMessageToFlow_ThreadUnsafe("errors", e);
  }

  public abstract void transitionToState(String s, boolean transactional)
      throws StateMachineException, TimeoutException, CoordinationException;

  public void transitionToState(String s) throws StateMachineException,
      CoordinationException, TimeoutException {
    transitionToState(s, false);
  }

  public Collection<Operation> prevNonLoopOperations() {
    return this.getTopFlow().graph().nonLoopOperationsTo(this);
  }

  /**
   * @throws OperationException
   */
  public Operation prevNonLoopOperation() throws OperationException {
    final Iterator<Operation> iter = this.prevNonLoopOperations().iterator();
    final Operation op = iter.next();
    assert (op != null);
    if (iter.hasNext()) {
      throw (OperationException) new OperationException(this).setAllMessages("Did not expect more than one prev operation for " + this.instanceName() + ". Prev ops: " + this.prevNonLoopOperations());
    }
    return op;
  }

  public Collection<Operation> prevOperations() {
    return this.getTopFlow().graph().operationsTo(this);
  }

  public Collection<Operation> nextOperations() {
    return this.getTopFlow().graph().operationsFrom(this);
  }

  public Collection<Operation> nextNonLoopOperations() {
    return this.getTopFlow().graph().nonLoopOperationsFrom(this);
  }

  public Collection<Connection> nextNonLoopConnections() {
    return this.getTopFlow().graph().nonLoopConnectionsFrom(this);
  }

  public Connection nextNonLoopConnection() throws OperationException {
    final Iterator<Connection> iter = this.nextNonLoopConnections().iterator();
    final Connection conn = iter.next();
    assert (conn != null);
    if (iter.hasNext()) {
      throw (OperationException) new OperationException(this).setAllMessages("Did not expect more than one next connection for op: "+namespaceName()+". Next conns: " + this.nextNonLoopConnections());
    }
    return conn;
  }

  public Connection prevNonLoopConnection() throws OperationException {
    final Iterator<Connection> iter = this.prevNonLoopConnections().iterator();
    final Connection conn = iter.next();
    assert (conn != null);
    if (iter.hasNext()) {
      throw (OperationException) new OperationException(this).setAllMessages("Did not expect more than one prev connection for op: "+namespaceName()+". Prev conns: " + this.prevNonLoopConnections());
    }
    return conn;
  }

  public Collection<Connection> prevNonLoopConnections() {
    return getTopFlow().graph().nonLoopConnectionsTo(this);
  }

  public Collection<Connection> prevConnections() {
    return getTopFlow().graph().connectionsTo(this.namespaceName());
  }

  public Collection<Connection> nextConnections() {
    return getTopFlow().graph().connectionsFrom(this.namespaceName());
  }

  public Operation nextNonLoopOperation() throws OperationException {
    final Iterator<Operation> iter = this.nextNonLoopOperations().iterator();
    final Operation op = iter.next();
    assert (op != null);
    if (iter.hasNext()) {
      throw (OperationException) new OperationException(this).setAllMessages("Did not expect more than one next operation for " + this.instanceName()+". Next ops: "+this.nextNonLoopOperations());
    }
    return op;
  }

  public abstract String getState();

  public boolean inState(String... states) {
    for (String s : states) {
      if (s.equalsIgnoreCase(getState())) {
        return true;
      }
    }
    return false;
  }

  /***
   * 
   */
  public void addExpectedFields(final String stream, final Fields fields) throws OperationException {
    if (outputStreams().contains(stream) == false) {
      throw (OperationException) new OperationException(this).setAllMessages("The stream " + stream + " has not been declared. Declared: " + outputStreams().toString());
    }
    if (_expectedFields.containsKey(stream) == false) {
      _expectedFields.put(stream, new Fields());
    }
    _expectedFields.get(stream).addAll(fields);
  }

  /***
   * 
   */
  public Fields getExpectedFields(String stream) {
    final Fields fields = _expectedFields.get(stream);
    return fields == null ? new Fields() : fields;
  }

  /**
   * 
   */
  public List<String> outputStreams() {
    return this.getTopFlow().graph().streamsFrom(this);
  }

  /**
   * 
   */
  public String defaultStream() throws DefaultStreamException {
    try {
      final Iterator<String> iter = outputStreams().iterator();
      final String stream = iter.next();
      assert (stream != null);
      if (iter.hasNext()) {
        throw new DefaultStreamException();
      }
      return stream;
    } catch (NoSuchElementException | DefaultStreamException e) {
      throw (DefaultStreamException) new DefaultStreamException(e)
          .setUserMessage("You must explicitly declare the stream to emit to. Expected: "
              + outputStreams().toString());
    }
  }

  /**
   * 
   */
  public void onFinalizeDeclare() throws OperationException,
      InterruptedException {
  }

  public int getMaxParallelism() {
    return this.getLocalConfig().get("parallelism", DEFAULT_MAX_PARALLELISM);
  }

  public Operation setMaxParallelism(int v) {
    this.getLocalConfig().set("parallelism", v);
    return this;
  }

  public int getTargetParallelism() {
    return _targetParallelism;
  }

  public Operation setTargetParallelism(int v) {
    _targetParallelism = v;
    return this;
  }
  
  public boolean getParallelismOverriden() {
    return _parallelismOverriden;
  }

  public Operation setParallelismOverriden(boolean v) {
    _parallelismOverriden = v;
    return this;
  }


  /** Create a a snapshot for suspension */
  public JSONObject createSnapshot() {
    return new JSONObject();
  };

  /** Apply a snapshot for resumption */
  public void applySnapshot(JSONObject snapshot) {
  }


  private void applySnapshotIfExists_ThreadUnsafe() {
    // Apply a snapshot if it exists
    _operationLogger.writeLog("checking for snapshot...", OperationLogger.LogPriority.SYSTEM);

    try {
      Utils.retryUnchecked(3, new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          try {
            
            String snapshotJSON = Universe.instance().dfsService().readFileAsString(s3SnapshotKey());
            if (snapshotJSON != null) {
              _operationLogger.writeLog("found snapshot, applying...", OperationLogger.LogPriority.SYSTEM);
              JSONObject snapshot = JSONUtil.parseObj(snapshotJSON);
              applySnapshot(snapshot);
              _operationLogger.writeLog("applied snapshot", OperationLogger.LogPriority.SYSTEM);

              // Delete old snapshot
              _operationLogger.writeLog("deleting old snapshot...", OperationLogger.LogPriority.SYSTEM);
              
              Universe.instance().dfsService().deleteFile(s3SnapshotKey());
              _operationLogger.writeLog("deleted snapshot", OperationLogger.LogPriority.SYSTEM);

            }
          } catch (IOException e) {
            throw (OperationException) new OperationException(Operation.this, e).setAllMessages("An error occurred while handling operation snapshot.");
          }
          return null;
        }
      });
    } catch (Exception e) {
      _operationLogger.writeLog("error applying snapshot: " + e.getMessage(), OperationLogger.LogPriority.ERROR);
    }
  }


  private String s3SnapshotKey() {
    return
        Utils.prefixKey("flows/" + this.getContainerFlow().getId() + "/snapshots/" + instanceName());
  }


  /***
   * 
   * @throws OperationException
   */
  public void handlePause() throws OperationException {

    // transition to PAUSING
    try {
      if (!getState().equalsIgnoreCase("ERROR"))
        transitionToState("PAUSING");
    } catch (StateMachineException | CoordinationException | TimeoutException e) {
      _operationLogger.writeLog("An error occurred while transitioning to PAUSING: " + e.getMessage(), OperationLogger.LogPriority.ERROR);
    }

    // Spin on queue for emitting
    // TODO add timeout
    while (getQueueCount() > 0) {
      Utils.sleep(1000);      
    }
  
    // upload snapshot
    _operationLogger.writeLog("Creating snapshot", OperationLogger.LogPriority.SYSTEM);
    Utils.retryUnchecked(3, new Callable<Void>() {
      @Override
      public Void call() throws OperationException {
        JSONObject snapshot = createSnapshot();

        if (!snapshot.isEmpty()) {
          try {

            _operationLogger.writeLog("created snapshot, uploading to S3... at " + s3SnapshotKey(), OperationLogger.LogPriority.SYSTEM);
            Universe.instance().dfsService().writeFile(s3SnapshotKey(), snapshot.toString());
            _operationLogger.writeLog("Uploaded snapshot ", OperationLogger.LogPriority.SYSTEM);
          } catch (IOException | S3Exception | InterruptedException e) {
            throw (OperationException) new OperationException(Operation.this, e).setAllMessages("An error occured during snapshot creation.");
          }
        }
        return null;
      }
    });

    // transition to PAUSED
    try {
      if (!getState().equalsIgnoreCase("ERROR"))
        transitionToState("PAUSED");
    } catch (StateMachineException | CoordinationException | TimeoutException e) {
      _operationLogger.writeLog("An error occurred while transitioning to PAUSED: " + e.getMessage(), OperationLogger.LogPriority.ERROR);
    }
  }
  
  private int getQueueCount(){
    
    if (_collector instanceof CoordinatedOutputCollector) {
      Triplet<Integer, Integer, Integer> queues = ((CoordinatedOutputCollector) _collector)
    .getUnackedAndLocalQueueAndRemoteQueeuCount_ThreadUnsafe();
      int unacked = queues.getValue0();
      int local = queues.getValue1();
      int remote = queues.getValue2();
      _operationLogger.writeLog("Queue Sizes = Unacked: " + unacked + " Local: " + local + " Remote: "
          + remote, OperationLogger.LogPriority.SYSTEM);
      return unacked + local + remote;
    }
    
   return 0;
  }

  /***
   * 
   * @throws OperationException
   */
  public void handleResume() throws OperationException {

    _operationLogger
        .writeLog("resuming...", OperationLogger.LogPriority.SYSTEM);

    // apply snapshot
    applySnapshotIfExists_ThreadUnsafe();


    // resume the operation
    try {
      if (!getState().equalsIgnoreCase("ERROR"))
        transitionToState("EMITTING");
    } catch (StateMachineException | CoordinationException | TimeoutException e) {
      _log.warn("An error occured while trying to resume " + e.getMessage());
    }
  }

  /***
   * known aliases this operation is emitting
   */
  @SuppressWarnings("unchecked")
  public Map<String, String> getAliases() throws OperationException,
      InterruptedException {
    return Collections.EMPTY_MAP;
  }

  /**
   * 
   */
  public boolean isAlive() {
    return true;
  }

  /***
   * 
   */
  @NonNullByDefault
  public void setState(String key, Object val) throws InterruptedException,
      CoordinationException {
    Universe.instance().state()
        .setState(operationStateKey() + SOURCE_STATE_KEY_PREFIX + key, val);
  }

  /***
   * 
   */
  public final <T> T getState(String key, T def) throws InterruptedException,
      CoordinationException {
    return Universe.instance().state()
        .getState(operationStateKey() + SOURCE_STATE_KEY_PREFIX + key, def);

  }

  /****
   * 
   */
  public final <T> T getState(String key) throws InterruptedException,
      CoordinationException {
    return Universe.instance().state()
        .getState(operationStateKey() + SOURCE_STATE_KEY_PREFIX + key);
  }

  /***
   * 
   */
  public void onBatchCompleting(final Object batchId) throws OperationException {
    // Do nothing by default
  }

  /***
   * Called by OutputCollector to see if okay to complete the current batch.
   * Aggregators override
   */
  public boolean permissionToCompleteBatch(Object batchId) {
    return true;
  }

  /**
   * Wrapper around prepare()...
   */
  public final void handlePrepare(final OutputCollector collector) throws InterruptedException, OperationException, OperationDeadException {
    try {
      try {

        // INIT
        _collector = collector;
        markBeginActivity();
        _errorStrategy = Universe.instance().errorStrategyFactory()
            .createOperationStrategy(Operation.this);

        /**************************************************
         ** INITIAL STAGE *********************************
         **************************************************/
        Utils.executeWithin(_initial_stage_timeout_ms, new Callable<Void>() {

          @Override
          public Void call() throws OperationException, InterruptedException, CoordinationException, TimeoutException, HeartbeatException {

            // Init
            Benchmark.markBegin("operation.prepare.initial");
            _log.info("operation instance is starting prepare(): " + instanceName());
            Lock lock = Universe.instance().state().lock("flow_instance_index_" + lockPrefix());
            if (lock == null) throw new NullPointerException("null lock?: " + Universe.instance().state());

            // Create an executor for hearts & state services
            _executor = Utils.createPrefixedExecutorPool("flow-" + topFlowId() + "-operation-" + instanceName());
            
            try {

              // Get the next instance id..
              final Integer lastInstanceIndex = Universe.instance().state().getState(operationStateKey() + "/last_instance", 0);
              final Integer nextInstanceIndex = lastInstanceIndex.intValue() + 1;
              Universe
                  .instance()
                  .state()
                  .setState(operationStateKey() + "/last_instance", nextInstanceIndex);
              _instanceIndex = nextInstanceIndex;

            } finally {
              lock.release();
            }

            _log.info("using instance id: " + instanceName());


            // Prepare the logger... Tell the state store where the logger islocated..
            _operationLogger = Universe.instance().loggerFactory().logger(topFlowId(), instanceName(), getTopFlow().getFlowConfig().getAuthToken());

            // Start the heartbeat
            _heartbeat = Heartbeat.create(Operation.this, _executor);
            reportInfo(); // report to flow that we are alive

            // Start logging to the user...
            _log.info("we are a : " + type());
            _operationLogger.writeLog("Starting new operation instance",
                OperationLogger.LogPriority.STARTUP);

            // Handle settings pubsubs
            _log.info("registering pubsubs");

            // Create the error strategy
            _emitCount = 0;
            _consumedCount = 0;

            // Starting State
            watchForFlowCommands();
            Benchmark.markEnd("operation.prepare.initial");
            return null;
          }

        });

        /**************************************************
         ** PRE_PREPARE STAGE *****************************
         **************************************************/
        Utils.executeWithin(_pre_prepare_stage_timeout_ms,
            new Callable<Void>() {
              @Override
              public Void call() throws OperationException,
                  InterruptedException {

                Benchmark.markBegin("operation.prepare.pre_prepare");
                _log.info("beginning pre-prepare stage...");
                prePrepare();

                _log.info("done with pre-prepare");
                Benchmark.markEnd("operation.prepare.pre_prepare");
                return null;

              }
            });

        /**************************************************
         ** PREPARE STAGE *****************************
         **************************************************/
        Utils.executeWithin(_prepare_stage_timeout_ms, new Callable<Void>() {
          @Override
          public Void call() throws MultiLangException, OperationException, InterruptedException {

            _log.info("begin prepare stage");
            Benchmark.markBegin("operation.prepare.actual");

            prepare();

            _log.info("done with prepare stage");
            Benchmark.markEnd("operation.prepare.actual");
            return null;
          }

        });

      } catch (ExecutionException e) {
        logger().error("Critical error in prepare stage for " + instanceName() + ".");
        if (!Universe.instance().env().isProd()) {
          logger().error("Internal error: " + ExceptionUtils.getStackTrace(e));
        }
        handleFatalError(e);

      } catch (TimeoutException e) {
        handleFatalError(new OperationException(this, e).setUserMessage("Prepare timeout exceeded."));
      }

      try {
        if (this.getState().equals("ERROR")) {
          _operationLogger.writeLog("Error detected during prepare phase", OperationLogger.LogPriority.ERROR);
        } else {
          // Success
          postPrepare();
          _operationLogger.writeLog("Prepare complete", OperationLogger.LogPriority.STARTUP);
        }
      } catch (Exception e) {
        handleFatalError(e);
      }
    } catch (FakeLocalException e) {
      e.printAndWait();
    } finally {
      markEndActivity();
    }
  }

  /***
   * 
   * Functions calling reactor functions directly.
   * 
   */
  private final void watchForFlowCommands() throws OperationException, InterruptedException {
    // _log.info("before watchForFlowCommands");
    try {
      _flowCommandWatcher  = Universe
          .instance()
          .state()
          .watchForMessage(_executor, flowStateKey() + "/operation_commands",
              new MessageHandler() {
                @Override
                public final void handleNewMessage(String key, Object command)
                    throws OperationException, InterruptedException {

                  // Let subclasses handle it...
                  _log.info(Operation.this.instanceName() + " received operation_command: " + command);
                  // System.err.println("foo");
                  try {
                    handleFlowCommand((String) command);
                  } catch (Exception e) {
                    throw (OperationException) new OperationException(Operation.this, e).setAllMessages("An error occurred while handling the flow command: "+command+".");
                  }
                }

              });
    } catch (CoordinationException e) {
      throw new OperationException(this, e);
    }
  }
  
  
  private void stopWatchingFlowCommands() throws CoordinationException {
    if (_flowCommandWatcher != null) {
      _flowCommandWatcher.unsubscribe();
      _flowCommandWatcher = null;
    }
  }
  

  /***
   * 
   * @param command
   * @throws Exception
   */
  protected void handleFlowCommand(String command) throws Exception {
    if (command.equalsIgnoreCase("die")) {
      _operationLogger.writeLog("Received command to shut down.", OperationLogger.LogPriority.RUN);
      handleCleanup();

    } else if (command.equalsIgnoreCase("report")) {

      _log.info("recieved report command");
      reportInfo();
      sendMessageToFlow_ThreadUnsafe("state", this.getState());

    } else if (command.equalsIgnoreCase("pause")) {

      // Pause the operation
      handlePause();

    } else if (command.equalsIgnoreCase("resume")) {

      // Resume the operation
      handleResume();

    }
  }

  public boolean getOperationShouldMerge() {
    return this._mergeIOFields;
  }

  public void setOperationShouldMerge(boolean b) {
    this._mergeIOFields = b;
  }

  public void addNamespacePrefix(String prefix) {
    if (_namespacePrefix == null || _namespacePrefix.equals("")) {
      _namespacePrefix = prefix;
    } else {
      _namespacePrefix = prefix + "." + _namespacePrefix;
    }
  }

  public String namespacePrefix() {
    return this._namespacePrefix;
  }

  @Override
  public String toString() {
    return "[" + this.type() + ":" + this.instanceName() + "]";
  }

  public String prefixifyStreamName(String stream) {
    // NOTE: overwritten in ComponentOutput
    if (namespacePrefix() != null && !namespacePrefix().isEmpty()) {
      return namespacePrefix() + "." + stream;
    } else {
      return stream;
    }
  }

  /***
   * Returns a sorted list of parent flows, from the top to the bottom
   * 
   * @return
   */
  public List<Flow> getParentFlowsTopDown() {
    Flow container = this._containerFlow;
    List<Flow> ret = Lists.newLinkedList();
    while (container != null) {
      ret.add(container);
      container = container.getParentFlow();
    }
    return Lists.reverse(ret);
  }

  public void mergeNewConfig(UserConfig config) {
    this._operationConfig.setAll(config);
  }

  public OperationConfig getMergedConfig() {
    // Get's the fully merged config
    OperationConfig conf = new OperationConfig(_operationConfig);
    for (Flow flow : getParentFlowsTopDown()) {
      conf.setAll(flow.getFlowConfig());
    }
    return conf;
  }

  public OperationConfig getLocalConfig() {
    return this._operationConfig;
  }

  public void setIncomingRouteByFields(List<String> fields) {
    _incomingRouteByFields = new Fields(fields);
  }
  
  public void setIncomingRouteByFields(Fields fields) {
    _incomingRouteByFields = fields;
  }

  public Fields getIncomingRouteByFields() {
    return _incomingRouteByFields;
  }

  public boolean hasIncomingRouteByFields() {
    return _incomingRouteByFields != null;
  }



  public void setActualParallelism(int parallelism) {
    _actualParallelism = parallelism;
  }
  
  public int getActualParallelism() {
    if (_actualParallelism == null) throw new IllegalStateException("actualParallelism hasn't been set yet.");
    return _actualParallelism;
  }

  public boolean hasSiblingInstances() {
    return getActualParallelism() > 1;
  }


  public Long getActivityTimeout() {
    return _activityTimeout;
  }


  public void onEnterPressureState() {
    this.logger().info("Tuples are backing up. Slowing down...");
  }
  
  public void onLeavePressureState() {
    this.logger().info("Tuple backlog cleared. Speeding up...");
  }




}
