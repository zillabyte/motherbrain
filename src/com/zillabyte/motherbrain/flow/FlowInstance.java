package com.zillabyte.motherbrain.flow;

import java.io.Serializable;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import net.sf.json.JSONObject;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;
import org.eclipse.jdt.annotation.Nullable;
import org.javatuples.Pair;

import com.google.common.collect.Maps;
import com.zillabyte.motherbrain.api.APIException;
import com.zillabyte.motherbrain.coordination.AskHandler;
import com.zillabyte.motherbrain.coordination.MessageHandler;
import com.zillabyte.motherbrain.coordination.Watcher;
import com.zillabyte.motherbrain.flow.operations.Operation;
import com.zillabyte.motherbrain.flow.operations.OperationLogger;
import com.zillabyte.motherbrain.flow.operations.OperationMessage;
import com.zillabyte.motherbrain.top.LocalServiceMain;
import com.zillabyte.motherbrain.top.MotherbrainException;
import com.zillabyte.motherbrain.universe.Config;
import com.zillabyte.motherbrain.universe.Universe;
import com.zillabyte.motherbrain.utils.JSONUtil;
import com.zillabyte.motherbrain.utils.SerializableMonitor;
import com.zillabyte.motherbrain.utils.Utils;

public class FlowInstance implements Serializable {

  static final long serialVersionUID = 6070760258622015224L;

  static final long RECOVER_RESPONSE_TIMEOUT_MS = TimeUnit.MILLISECONDS.convert(10, TimeUnit.SECONDS);
  static final long STATE_CHANGE_MONITOR_TIMEOUT_MS = TimeUnit.MILLISECONDS.convert(1, TimeUnit.SECONDS);
  static final long STATE_POLLER_PERIOD_MS = TimeUnit.MILLISECONDS.convert(2, TimeUnit.MINUTES);
  static final int MAX_ERROR_HISTORY_PER_INSTANCE = 10;
  static final int MAX_ERROR_HISTORY = 20;
  static final long SUSPEND_TIMEOUT = Config.getOrDefault("flow.instance.suspend.timeout.ms", 1000L * 60 * 5);
  static final DateFormat dateFormat = DateFormat.getDateInstance();
  static final Logger _log = Utils.getLogger(FlowInstance.class);
  
  static final ExecutorService _executor = Utils.createPrefixedExecutorPool("flow-instance");
  

  private final App _flow;
  private final OperationLogger _logger;
  private final FlowOperationInstanceCollection _instances;


  private FlowStateCoordinator _stateCoordinator;


  // Flag for instructing bufferSinks to stop sinking
  private boolean _bufferNotified = false;

  transient @Nullable volatile Watcher _messageWatcher;
  transient @Nullable volatile Watcher _emitPermissionWatcher;

  // final Pruner _pruner = new Pruner.Nimbus(this);

  private SerializableMonitor _stateMonitor = new SerializableMonitor();
  private SerializableMonitor _stateChangeMonitor = new SerializableMonitor();
  private SerializableMonitor _statePollerMonitor = new SerializableMonitor();

  private FlowState _state = FlowState.INITIAL;

  private ScheduledFuture<?> _statePollerFuture;




  /***
   * 
   * @param flow
   */
  public FlowInstance(final App flow) {
    this._flow = flow;
    this._logger = flow.getLogger();
    this._instances = new FlowOperationInstanceCollection();
    this._stateCoordinator = new FlowStateCoordinator(_flow);
    this._bufferNotified = false;
    applySnapshotIfExists_ThreadUnsafe();
  }


  public App flow() {
    return _flow;
  }


  public String id() {
    return _flow.getId();
  }


  public String name() {
    return _flow.getName();
  }


  private String flowStateKey() {
    return _flow.flowStateKey();
  }


  public final Set<String> operationNames() {
    final HashSet<String> names = new HashSet<>();
    for(final Operation o : _flow.getOperations()) {
      names.add(o.namespaceName());
    }
    return names;
  }


  public FlowStateCoordinator getFlowStateCoordinator() {
    return _stateCoordinator;
  }


  public FlowOperationInstanceCollection instances() {
    return _instances.clone();
  }


  public FlowState getFlowState() {
    synchronized(_stateMonitor) {
      return _state;
    }
  }


  private Set<String> getWorkerIPs() {
    final Set<String> workers = new HashSet<>();
    for(FlowOperationInstance inst : _instances) {
      final Map<String, Object> instanceInfo = inst.getInfo();
      final String workerHost = (String) instanceInfo.get("host");
      if(workerHost != null) {
        workers.add(workerHost);
      }
    }
    return workers;
  }


  public Set<String> getOperationsThatAreNotAlive() {
    return instancesSetBuilder().getNotAliveOperationNames();
  }


  public boolean allOperationsAlive() {
    return instancesSetBuilder().allOperationsAlive();
  }


  private FlowInstanceSetBuilder instancesSetBuilder() {
    return FlowInstanceSetBuilder.buildFromOperationInstanceStates(this.instances());
  }  




  public void maybeUpdateFlowState() {
    synchronized(_stateMonitor) {
      FlowState newState = _stateCoordinator.maybeGetNewFlowState(instancesSetBuilder(),  _state);
      if(newState != _state) {
        if(newState == FlowState.ERROR) {
          killImpl(FlowState.ERROR);
        } else {
          transitionToState(newState);
        }
      }
    }
  }


  public void transitionToState(FlowState target) {
    synchronized(_stateMonitor) {
      try {
        FlowState oldState = _state;
        _state = StateMachineHelper.transition(_state, target);  
        if(_state != oldState) {
          if (inTerminalState() && _bufferNotified == false) {
            _bufferNotified = true;
            flushBufferProducers();
          }
          notifyOfNewState(_state.toString());
        }
      } finally {
        synchronized (_stateChangeMonitor) {
          _stateChangeMonitor.notifyAll();
        }
      }
    }
  }

  
  private void flushBufferProducers() {
    for(FlowOperationInstance i : this.instances()) {
      if("sink".equals(i.getType())){
        if (i.getInfo().containsKey("sink_topic")) {
          Universe.instance().bufferClientFactory().createFlusher().flushProducers(i.getInfo().get("sink_topic").toString());
        }
      }
    }
  }


  private void notifyOfNewState(String newState) {
    // Only notify API of new state if we're an app, not if we're a component rpc
    try {
      Universe.instance().api().postFlowState(id(), newState, _flow._flowConfig.getAuthToken());
    } catch(APIException ex) {
      throw new RuntimeException(ex);
    }
    final String message = "Transitioned to state "+ newState.toString();
    _logger.writeLog(message, OperationLogger.LogPriority.RUN);
  }


  //  OperationInstanceState<FlowInstance, ConcurrentHashMap<String, Object>> getInstanceConfig(final String instKey, final String initialState) {
  //    final OperationInstanceState<FlowInstance, ConcurrentHashMap<String, Object>> instState = FlowInstance.makeOperationState(initialState);
  //    return Utils.putIfAbsent(_instances, instKey, instState);
  //  }
  //
  //  static OperationInstanceState<FlowInstance, ConcurrentHashMap<String, Object>> makeOperationState(final String initialState) {
  //    final ConcurrentHashMap<String, Object> info = new ConcurrentHashMap<>();
  //    final HashMap<String, String> stats = new HashMap<>();
  //    final OperationInstanceState<FlowInstance, ConcurrentHashMap<String, Object>> instState = OperationInstanceState.makeOperationInstanceState(initialState, stats, info);
  //    /*
  //     * Seems to be a bug that I even have to assert this.
  //     */
  //    assert (instState != null);
  //    return instState;
  //  }
  //
  //  @Override


  public Map<String, Object> buildDetailsMap() {
    final Map<String, Object> m = Maps.newHashMap();
    m.put("instances", this._instances.getJSONDetails());
    m.put("flow_id", id());
    m.put("flow_name", name());
    m.put("flow_state", getFlowState());

    final Map<String, Long> heartbeats = Maps.newHashMap();
    final Map<String, Map<String, Object>> instances = Maps.newHashMap();
    for (FlowOperationInstance inst : _instances) {

      final Map<String, Object> details = Maps.newHashMap();
      final Map<String, Object> info = inst.getInfo();
      final Map<String, String> stats = inst.getStats();      

      details.put("info", info);
      details.put("stats", stats);
      details.put("state", inst.getState());

      final ArrayList<Map<String, Object>> recentErrors = new ArrayList<>();
      for (Pair<Exception, Long> p: inst.getRecentErrorsWithDate()) {
        final Exception e = p.getValue0();
        final Long d = p.getValue1();
        final HashMap<String, Object> errorMap = new HashMap<>();
        final MotherbrainException me = Utils.getRootException(e, MotherbrainException.class);
        errorMap.put("internal_message", e.toString());
        errorMap.put("stack_trace", ExceptionUtils.getFullStackTrace(e));
        if (me != null) {
          errorMap.put("message", me.getMessage());
          errorMap.put("date", dateFormat.format(Utils.valueOf(me.getDate())));
        }
        errorMap.put("processed_date", dateFormat.format(d));
        recentErrors.add(errorMap);
      }
      details.put("recent_errors", recentErrors);
      instances.put(inst.getId(), details);
      heartbeats.put(inst.getId(), Utils.valueOf(inst.lastUpdateTime()));
    }
    m.put("instances", instances);
    m.put("heartbeats", heartbeats);
    if(_state != FlowState.ERROR) m.put("waiting_to_come_online", getOperationsThatAreNotAlive());

    return m;
  }



  public FlowInstance waitForState(FlowState... state) {
    waitForState(-1, state); // wait forever
    return this; // for chaining
  }




  public boolean waitForState(final long timeoutMillis, final FlowState... states) {
    final long timeoutNanos = TimeUnit.NANOSECONDS.convert(timeoutMillis, TimeUnit.MILLISECONDS);
    final boolean infiniteLoop = timeoutMillis < 0;
    final long monitorTimeoutMillis;
    if (infiniteLoop || STATE_CHANGE_MONITOR_TIMEOUT_MS < timeoutMillis) {
      monitorTimeoutMillis = STATE_CHANGE_MONITOR_TIMEOUT_MS;
    } else {
      monitorTimeoutMillis = timeoutMillis;
    }


    _log.info("waiting for flow " + id()  + " to change state to: " + Arrays.toString(states));

    final long start = System.nanoTime();
    while(true) {
      final FlowState curState = getFlowState();
      for(final FlowState state : states) {
        if (curState == state) {
          _log.debug("flow " + id() + " reached state : " + state.toString());
          return true;
        }
      }
      if (infiniteLoop) {
        if (curState == FlowState.ERROR) {
          throw new RuntimeException("Reached state ERROR unexpectedly! Expected " + Arrays.toString(states));
        }
      } else if (System.nanoTime() - start >= timeoutNanos) {
        break;
      }
      synchronized(_stateChangeMonitor) {
        try {
          _stateChangeMonitor.wait(monitorTimeoutMillis);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }
    if (!Thread.currentThread().isInterrupted()) {
      _log.info("flow " + id() + " did not reach state " + Arrays.toString(states) + " in time");
    }
    return false;
  }


  public FlowInstance waitUntilDone() {
    this.waitForState(FlowState.WAITING_FOR_NEXT_CYCLE, FlowState.ERROR);
    return this;
  }


  public boolean waitUntilAllOperationsAlive(final long timeoutMillis, final FlowState... states) {
    final long timeoutNanos = TimeUnit.NANOSECONDS.convert(timeoutMillis, TimeUnit.MILLISECONDS);
    final boolean infiniteLoop = timeoutMillis < 0;
    final long monitorTimeoutMillis;
    if (infiniteLoop || STATE_CHANGE_MONITOR_TIMEOUT_MS < timeoutMillis) {
      monitorTimeoutMillis = STATE_CHANGE_MONITOR_TIMEOUT_MS;
    } else {
      monitorTimeoutMillis = timeoutMillis;
    }

    final long start = System.nanoTime();
    while (!this.allOperationsAlive() && inState(states)) {
      if (!infiniteLoop && System.nanoTime() - start >= timeoutNanos) {
        return false;
      }
      synchronized (_stateChangeMonitor) {
        try {
          _stateChangeMonitor.wait(monitorTimeoutMillis);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }

    return true;
  }


  public FlowInstance waitUntilStarted() {
    this.waitForState(FlowState.STARTED);
    return this;
  }


  public boolean inState(FlowState... states) {
    final FlowState currentState = getFlowState();
    for(final FlowState s : states) {
      if (s == currentState) {
        return true;
      }
    }
    return false;
  }
  
  public boolean inTerminalState() {
    return inState(FlowState.ERRORING ,FlowState.ERROR, FlowState.WAITING_FOR_NEXT_CYCLE, FlowState.KILLING, FlowState.KILLED, FlowState.RETIRED, FlowState.RETIRING);
  }

  void removeAllFlowState() {
    _log.info("flow prep: deleting existing keys for flowId: " + id());
    Universe.instance().state().removeStateWithPrefix(flowStateKey());
  }

  void tellAllOperationsToDie() {
    _log.info("telling all operations to die!");
    Universe.instance().state().sendMessage(flowStateKey() + "/operation_commands", "die");
  }


  public void removeWatchers() {
    try {
      try {
        if (this._emitPermissionWatcher != null) {
          this._emitPermissionWatcher.unsubscribe();
        }
      } finally {
        if (this._messageWatcher != null) {
          this._messageWatcher.unsubscribe();
        }
      }
    } finally {
      if (_statePollerFuture != null) _statePollerFuture.cancel(false);
    }
  }
  /**
   * Snapshotting
   */

  public JSONObject createSnapshot(){
    JSONObject snapshot = new JSONObject();
    snapshot.put("flow_version", _flow.getVersion());
    return snapshot;
  };

  public void applySnapshot(JSONObject snapshot){
    _flow.setVersion(Integer.parseInt(snapshot.getString("flow_version")));
  };
  
  public void killImpl(final FlowState finalState) {
    try {
      _log.info("beginning kill sequence...");
      if(finalState.equals(FlowState.KILLED)) {
        _log.info("Should delete logs");
      }
      final FlowState dyingState;
      final FlowState deadState;
      switch (finalState) {
      case KILLED:     
        dyingState = FlowState.KILLING;
        deadState = FlowState.KILLED;
        break;
      case RETIRED:
        dyingState = FlowState.RETIRING;
        deadState = FlowState.RETIRED;
        break;
      case ERROR:
        dyingState = FlowState.ERRORING;
        deadState = FlowState.ERROR;
        break;   
      default:
        throw new RuntimeException("Invalid state sent to kill "+finalState);
      }

      try {
        synchronized(_stateMonitor) {
          if (this.inState(FlowState.KILLED, FlowState.ERROR, FlowState.RETIRED)) {
            return;
          }
          transitionToState(dyingState);
        }
      } catch(Exception e) {
        _log.warn("deepKill state error" + e);
      }

      synchronized(this) {
        /*
         * Clean up operations.
         */
        _log.info("Telling all ops to die...");
        tellAllOperationsToDie();
        /*
         * Remove the watchers.
         */
        _log.info("Removing watchers");
        removeWatchers();
        /*
         * Kill the storm topology.
         */
        Universe.instance().flowService().killFlow(this);
        /*
         * Remove state from Redis.
         */
        removeAllFlowState();
      }
        
      // clean up lxc on workers
      final Set<String> workers = getWorkerIPs();
      final ArrayList<Future<Void>> killFutures = new ArrayList<>(workers.size());
      try {
        for (final Future<Void> killFuture : killFutures) {
          killFuture.get();
        }
      } finally {
        for (final Future<Void> killFuture : killFutures) {
          killFuture.cancel(true);
        }
      }
      transitionToState(deadState);

    } catch (Exception e) {
      /*
       * Do nothing
       */
      _log.error("unable to kill storm topology!: " + e);
    } finally {
      _log.info("shutting down flow instance reactor");
    }
  }


  public void pause() {

    // transition to PAUSING
    transitionToState(FlowState.PAUSING);

    // upload snapshot
    _log.info("creating snapshot");

    Utils.retry(3, new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        JSONObject snapshot = createSnapshot();

        if (!snapshot.isEmpty()) {

          _log.info("created snapshot, uploading to S3... at " + dfsSnapshotKey());
          Universe.instance().dfsService().writeFile(dfsSnapshotKey(), snapshot.toString());
          _log.info("Uploaded snapshot ");

        }
        return null;
      }
    });

    // the flow instance will update its state once all operations have paused
    Universe.instance().state().sendMessage(flowStateKey() + "/operation_commands", "pause");

    // Wait until instance is paused
    waitUntilAllOperationsAlive(LocalServiceMain.OPERATION_STARTING_TIMEOUT_MS, FlowState.PAUSED);
  }

  public void resume() {

    Universe.instance().state().sendMessage(flowStateKey() + "/operation_commands", "resume");

    // apply snapshot
    applySnapshotIfExists_ThreadUnsafe();

    if (waitUntilAllOperationsAlive(LocalServiceMain.OPERATION_STARTING_TIMEOUT_MS, FlowState.STARTED, FlowState.RUNNING)){
      transitionToState(FlowState.RUNNING);  
    }

  }

  private void applySnapshotIfExists_ThreadUnsafe() {
    // Apply a snapshot if it exists
    _log.info("checking for snapshot...");

    Utils.retry(3, new Callable<Void>() {
      @Override
      public Void call() {

        String snapshotJSON = null;
        if(Universe.instance().dfsService().pathExists(dfsSnapshotKey())) {
          snapshotJSON = Universe.instance().dfsService().readFileAsString(dfsSnapshotKey());
        }

        if (snapshotJSON != null) {
          _log.info("found snapshot, applying...");
          JSONObject snapshot = JSONUtil.parseObj(snapshotJSON);
          applySnapshot(snapshot);
          _log.info("applied snapshot");

          // Delete old snapshot
          _log.info("deleting old snapshot...");

          Universe.instance().dfsService().deleteFile(dfsSnapshotKey());

          _log.info("deleted snapshot");

        }

        return null;
      }
    });

  }

  private String dfsSnapshotKey() {
    return Utils.prefixKey("flows/" + this.flow().getId() + "/snapshots/" + "flow");
  }

  public static FlowInstance recover(byte[] data) {
    throw new NotImplementedException();
    //    // Deserialize, Init
    //    FlowInstance inst = (FlowInstance) Utils.deserialize(data);
    //    try {
    //      inst.handleWatching();
    //    } catch (FlowException e) {
    //      throw new FlowRecoveryException(e);
    //    }
    //    inst._state = new State<>(FlowState.stateMachine, FlowState.RECOVERING, inst);
    //    inst._instances.clear();
    //    // Two scenarios: (a) we may have currently running operations; (b) operations are
    //    // not running (storm died, gamma rays, etc);  
    //    // For (a), we want to ping the network and see who is still alive;  If all operations
    //    // report online, then we can successuflly 'pick up' where we left off.  In case of (b)
    //    // we simply fail to recover;
    //    // See who is online... 
    //    try {
    //      Universe.instance().state().sendMessage(inst.flowStateKey() + "/operation_commands", "report");
    //      // See if anybody has responded...
    //      if (!inst.waitUntilAllOperationsAlive(RECOVER_RESPONSE_TIMEOUT_MS)) {
    //        FlowInstance._log.warn("timeout: unable to recover flow: " + inst.id());
    //        return inst;
    //      }
    //      // If we get here, then all operations are online and have reported in.
    //      // Next step, update our current status.
    //      inst.updateState();
    //    } catch (StateMachineException | FlowException | OperationException | CoordinationException e) {
    //      throw new FlowRecoveryException(e);
    //    }
    //    inst.initPollers();
    //    // Successfully recovered
    //    return inst;
  }



  /***
   * 
   * Functions directly calling reactor functions
   * 
   */
  public void initPollers() {
    _statePollerFuture = Utils.timerFromPool(STATE_POLLER_PERIOD_MS, new Runnable() {
      @Override
      public void run() {
        try {
          maybeUpdateFlowState();
        } catch (Exception e) {
          _log.error("unhandled poller exception: " + e + " \n " + ExceptionUtils.getFullStackTrace(e));
          if (e instanceof InterruptedException) {
            _statePollerFuture.cancel(false);
          }
        }
      }
    });

  }


  /***
   * Handles watching for all network based messages.  Currently, the state service is backed
   * by Redis and we wish to reduce the network load on it. 
   * @throws InterruptedException
   */
  public void handleWatching() {

    ///////////
    // WATCH FOR ASKS...
    ///////////

    // Called when an operation (sources) spins up and wants to know if it's OKAY to start emitting stuff.  See this.startNewCycle() below
    if (_emitPermissionWatcher != null) {
      _emitPermissionWatcher.unsubscribe();
    }

    _emitPermissionWatcher = Universe.instance().state().watchForAsk(_executor, flowStateKey() + "/emit_permission", new AskHandler() {
      @Override
      public Object handleAsk(String key, Object payload) {
        synchronized(_stateMonitor) {
          if (getFlowState() == FlowState.RUNNING) {
            return Boolean.TRUE;
          }
        }
        return Boolean.FALSE;
      }
    });

    ///////////
    // WATCH FOR MESSAGES...  
    ///////////

    if (_messageWatcher != null) {
      _messageWatcher.unsubscribe();
    }

    _messageWatcher = Universe.instance().state().watchForMessage(_executor, flowStateKey(), new MessageHandler() {
      @SuppressWarnings("unchecked")
      @Override
      public void handleNewMessage(final String key, final Object rawPayload) {

        final OperationMessage opMessage = (OperationMessage) rawPayload;
        final String id = opMessage.getInstanceName();
        final String command = opMessage.getCommand();
        final Object payload = opMessage.getMessage();
        _log.debug("handling a message for key " + key + " : command " + command);

        synchronized(this) {
          switch (command) {
          case "stats":
            _instances.getOrCreate(id).updateStats((Map<String, String>) payload);
            maybeUpdateFlowState();
            break;
          case "state":

            _instances.getOrCreate(id).updateState((String) payload);
            maybeUpdateFlowState();
            break;
          case "info":
            _instances.getOrCreate(id).updateInfo((Map<String, Object>) payload);
            maybeUpdateFlowState();
            break;
          case "errors":
            _instances.getOrCreate(id).addRecentError((Exception) payload);
            maybeUpdateFlowState();
            break;
          default:
            _log.warn("Received unknown command: " + command);
          }
        }
      }
    });
  }  



  public FlowInstance startNewCycle() {
    synchronized(_stateMonitor) {
      // we have a 'pull' architecture here rather than a 'push'. (that is, the operation instances
      // continually ask us if it's okay to start running, rather than us telling them to start)
      // Why? Because new operation instances can pop up at any time (especially after errors),
      // and it's easier to have them ask us for permission to start, rather than us synchronizing
      // a push-system with them.
      _log.info("starting new cycle: " + id());

      // make sure bufferSinks are notified regardless of outcome
      _bufferNotified = false;

      transitionToState(FlowState.RUNNING);

      return this;  // for chaining
    }
  }



  public void kill() {
    killImpl(FlowState.KILLED);
    _instances.clear();
  }



  public void retire() {
    killImpl(FlowState.RETIRED);
    _instances.clear();
  }


  public Integer version() {
    return _flow.getVersion();
  }


  public void start() throws Exception {
    handleWatching();
    transitionToState(FlowState.STARTING);
  }

  public void handlePostDeploy() {
    initPollers();
  }


  //  
  //  /***
  //   * 
  //   * Reactor Functions
  //   *
  //   */
  //  public class ExecuteStartNewCycle implements ReactorCallable {
  //    
  //    @Override
  //    public void call() throws FlowException {
  //      _log.info("starting new cycle: " + id());
  //      try {
  //        synchronized(_stateMonitor) {
  //          transitionToState(FlowState.RUNNING);
  //        }
  //      } catch (StateMachineException e) {
  //        throw new FlowException(_flow, e);
  //      }
  //    }
  //    
  //  }
  //  
  //  public class ExecuteKill implements CustomizableReactorFunction {
  //
  //    FlowState _finalState;
  //    
  //    @Override
  //    public void init(Object... args) {
  //      _finalState = (FlowState) args[0];
  //    }
  //    
  //    @Override
  //    public void call() throws FlowException, StateMachineException, InterruptedException, CoordinationException, ExecutionException {
  //      kill(_finalState);
  //    }
  //
  //    @Override
  //    public void cleanup() {
  //      // none
  //    }
  //  
  //  }
  //  
  //  public class ExecuteMessageHandler implements CustomizableReactorFunction {
  //
  //    String _key;
  //    Object _rawPayload;
  //
  //    @Override
  //    public void init(Object...args) {
  //      _key = (String) args[0];
  //      _rawPayload = args[1];
  //    }
  //
  //    @Override
  //    public void call() throws OperationException, InterruptedException, CoordinationException, StateMachineException, TimeoutException, FakeLocalException, IOException, MultiLangException, FlowException {
  //      final OperationMessage opMessage = (OperationMessage) _rawPayload;
  //      final String instKey = opMessage.getInstanceName();
  //      final String command = opMessage.getCommand();
  //      final Object payload = opMessage.getMessage();
  //      final boolean result;
  //      //      _log.info("handleWatching: received command: " + command + " for instance " + instKey + " with payload " + payload);
  //      switch (command) {
  //      case "stats":
  //        result = updateStats(instKey, payload);
  //        break;
  //      case "state":
  //        result = updateState(instKey, payload);
  //        break;
  //      case "info":
  //        result = updateInfo(instKey, payload);
  //        break;
  //      case "errors":
  //        result = updateErrors(instKey, payload);
  //        _log.info(getDetails());
  //        break;
  //      default:
  //        _log.warn("Received unknown command: " + command);
  //      }
  //    }
  //    
  //    @Override
  //    public void cleanup() {
  //      // none
  //    }
  //
  //  }
  //  
  //  public class ExecuteStatePoll implements ReactorCallable {
  //
  //    @Override
  //    public void call() {
  //      try {
  //        _pruner.handlePrune();
  //        maybeUpdateFlowState(instancesSetBuilder());;
  //      } catch (StateMachineException | FlowException | OperationException | CoordinationException e) {
  //        _log.error("unhandled poller exception: " + e + " \n " + ExceptionUtils.getFullStackTrace(e));
  //      } catch(InterruptedException e) {
  //        _reactor.stopPeriodic("STATE_POLLER");  
  //      }
  //    }
  //  }
  //  
}
