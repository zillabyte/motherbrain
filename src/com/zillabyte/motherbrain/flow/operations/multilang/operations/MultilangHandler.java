package com.zillabyte.motherbrain.flow.operations.multilang.operations;

import java.io.Serializable;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeoutException;

import net.sf.json.JSONObject;

import org.apache.log4j.Logger;

import com.zillabyte.motherbrain.container.ContainerEnvironmentHelper;
import com.zillabyte.motherbrain.container.ContainerException;
import com.zillabyte.motherbrain.container.ContainerWrapper;
import com.zillabyte.motherbrain.flow.MapTuple;
import com.zillabyte.motherbrain.flow.config.OperationConfig;
import com.zillabyte.motherbrain.flow.error.strategies.FakeLocalException;
import com.zillabyte.motherbrain.flow.operations.Operation;
import com.zillabyte.motherbrain.flow.operations.OperationDeadException;
import com.zillabyte.motherbrain.flow.operations.OperationException;
import com.zillabyte.motherbrain.flow.operations.OperationLogger;
import com.zillabyte.motherbrain.flow.operations.multilang.MultiLangCleaner;
import com.zillabyte.motherbrain.flow.operations.multilang.MultiLangException;
import com.zillabyte.motherbrain.flow.operations.multilang.MultiLangProcess;
import com.zillabyte.motherbrain.flow.operations.multilang.MultiLangProcessException;
import com.zillabyte.motherbrain.flow.operations.multilang.MultiLangProcessGeneralOperationObserver;
import com.zillabyte.motherbrain.flow.operations.multilang.MultiLangProcessTupleObserver;
import com.zillabyte.motherbrain.top.MotherbrainException;
import com.zillabyte.motherbrain.universe.Universe;
import com.zillabyte.motherbrain.utils.Utils;

/***
 * This class exists because we want a DRY way to handle all the multilang
 * stuff, but can't use inheritance because each multialng operation inherits
 * from Function, Source, Sink, etc;
 * 
 * @author jake
 * 
 */
public class MultilangHandler implements Serializable {

  private static final long serialVersionUID = 430771401006436180L;
  private static final Logger _log = Utils.getLogger(MultilangHandler.class);

  private Operation _operation;
  private ContainerWrapper _container;
  private MultiLangProcess _process;
  private MultiLangProcessGeneralOperationObserver _processGeneralObserver;
  private MultiLangProcessTupleObserver _processTupleObserver;
  private Map<String, String> _knownAliases;

  public MultilangHandler(Operation op, JSONObject nodeSettings, ContainerWrapper container) {
    _operation = op;
    _container = container;
    _operation.setOperationShouldMerge(nodeSettings.optString("output_format", "replace").equalsIgnoreCase("merge"));
    if (nodeSettings.has("config")) {
      _operation.mergeNewConfig(OperationConfig.createFromJSON(nodeSettings.getJSONObject("config")));
    }
  }

  public static String getName(JSONObject nodeSettings) {
    return nodeSettings.getString("name");
  }

  public synchronized void prepare() throws MultiLangException {
    try {

      // Init
      _log.info("starting live run " + _operation.instanceName());
      if (_process != null)
        throw new MultiLangException(_operation, "the process has already been initialized");

      // Pull down the containers...
      Universe.instance().containerFactory().createSerializer().deserializeOperationInstance(_container, _operation.instanceName());

      // Start it up
      // TODO we need to explain to the container that we are starting it at the
      // instance level rather than the flow level
      _container.start();

      // Do a 'prep'. TODO: remove this, and make sure all dependencies are included in the deserialization
      _container.buildCommand()
          .withEnvironment(ContainerEnvironmentHelper.getCLIEnvironment(this._operation.getTopFlow().getFlowConfig()))
          .inFlowDirectory(_operation.getContainerFlow().getId())
          .withCLICommand("prep", "--mode", Universe.instance().env().toString())
          .withoutSockets()
          .withEnvironment("ZILLABYTE_PARAMS", _operation.getMergedConfig().toJSON().toString())
          .createProcess()
          .addLogListener(_operation.logger())
          .start()
          .waitForExit(1000L * 45);

      // Handshake
      _process = _container.buildCommand()
          .withEnvironment(ContainerEnvironmentHelper.getCLIEnvironment(this._operation.getTopFlow().getFlowConfig()))
          .inFlowDirectory(_operation.getContainerFlow().getId())
          .withCLICommand("live_run", _operation.userGivenName())
          .withSockets()
          .withEnvironment("ZILLABYTE_PARAMS", _operation.getMergedConfig().toJSON().toString())
          .createProcess()
          .addLogListener(_operation.logger())
          .start();

      _process.handleHandshake();
      _processGeneralObserver = new MultiLangProcessGeneralOperationObserver(_process, _operation);
      _processTupleObserver = new MultiLangProcessTupleObserver(_process, _operation);

      // Send the prepare message...
      _processTupleObserver.startWatching();
      _processGeneralObserver.mabyeThrowNextError();
      _process.writeMessageWithEnd("{\"command\": \"prepare\"}");
      _processTupleObserver.waitForDoneMessageWithoutCollecting();
      _processGeneralObserver.mabyeThrowNextError();

      // Clean up later...
      MultiLangCleaner.registerOperation(_process, _operation);
      _log.info("live run is running...");

    } catch (MotherbrainException | InterruptedException | TimeoutException ex) {
      _operation.logger().writeLog("Internal error while preparing operation", OperationLogger.LogPriority.ERROR);
      throw new MultiLangException(_operation, ex);
    }
  }

  public synchronized void cleanup(boolean destroyContainer) throws MultiLangException {
    try {
      if (_processGeneralObserver != null) {
        _processGeneralObserver.detach();
      }
      if (_process != null) {
        _process.destroy();
        _process = null;
      }
      if (destroyContainer) {
        _container.cleanup();
      }
    } catch (MultiLangProcessException | ContainerException e) {
      throw new MultiLangException(_operation, e);
    }
  }

  public void cleanup() throws MultiLangException {
    cleanup(true);
  }

  public synchronized boolean isAlive() {
    return _process.isAlive();
  }

  public MultiLangProcessTupleObserver tupleObserver() {
    return this._processTupleObserver;
  }

  public MultiLangProcessGeneralOperationObserver generalObserver() {
    return this._processGeneralObserver;
  }

  public ContainerWrapper getContainer() {
    return this._container;
  }

  public void writeMessage(String string) throws MultiLangException {
    try {
      _process.writeMessageWithEnd(string);
    } catch (InterruptedException | MultiLangProcessException e) {
      throw new MultiLangException(_operation, e);
    }
  }

  public void waitForDoneMessageWithoutCollecting() throws OperationException, InterruptedException {
    this._processTupleObserver.waitForDoneMessageWithoutCollecting();
  }

  public void startWatchingForTuples() {
    _processTupleObserver.startWatching();
  }

  public void stopWatchingForTuples() {
    _processTupleObserver.stopWatching();
  }

  public void maybeThrowNextError() throws OperationException {
    this._processGeneralObserver.mabyeThrowNextError();
  }

  public Object takeNextTuple() throws OperationException, InterruptedException {
    return _processTupleObserver.takeNextTuple();
  }

  public void onFinalizeDeclare() throws OperationException, InterruptedException {
    _knownAliases = _operation.prevNonLoopOperation().getAliases();
  }

  public void addAliases(MapTuple t) {
    for (Entry<String, String> e : _knownAliases.entrySet()) {
      final String key = e.getKey();
      final String value = e.getValue();
      t.addAlias(key, value);
    }
  }

  public void handleRestartingProcess() throws OperationException, FakeLocalException {
    synchronized (_operation) {
      _log.error("restarting process because of timeout.");
      try {
        _operation.markBeginActivity();
        cleanup(false);
        prepare();
      } catch (MultiLangException e1) {
        _operation.handleFatalError(e1);
      } finally {
        _operation.markEndActivity();
      }
    }
  }

  public void ensureAlive() throws OperationDeadException {
    if (this.isAlive() == false) {
      throw new OperationDeadException(this._operation, "The operation is dead");
    }
  }

  public static OperationConfig getConfig(JSONObject nodeSettings) {
    if (nodeSettings.containsKey("config")) {
      return OperationConfig.createFromJSON(nodeSettings.getJSONObject("config"));
    } else {
      return OperationConfig.createEmpty();
    }
  }

  

}
