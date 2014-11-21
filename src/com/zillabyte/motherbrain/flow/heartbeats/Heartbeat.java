package com.zillabyte.motherbrain.flow.heartbeats;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;

import com.zillabyte.motherbrain.flow.operations.Operation;
import com.zillabyte.motherbrain.universe.Config;
import com.zillabyte.motherbrain.utils.Log4jWrapper;
import com.zillabyte.motherbrain.utils.MeteredLog;
import com.zillabyte.motherbrain.utils.Utils;

public class Heartbeat {

  public final static Long DEFAULT_POLL_INTERVAL_MS = 1000L * 10;
  public final static Long DEFAULT_KILL_INTERVAL_MS = 1000L * 35;
  
  final long DEFAULT_HEARTBEAT_INTERVAL_MS = Config.getOrDefault("heartbeat.poll.interval", DEFAULT_POLL_INTERVAL_MS);
  final long HEARTBEAT_KILL_MS = Config.getOrDefault("heartbeat.kill.interval", DEFAULT_KILL_INTERVAL_MS);

  private Operation _op;
  private long _tickInterval;
  private ScheduledFuture<?> _timer;
  private ScheduledFuture<?> _errorTimer;

  private Log4jWrapper _log;
  private long _ticks = 0L;
  private long _lastHeartbeat = System.currentTimeMillis();
  private Exception _unhandledException = null;
  private Future<Void> _heavyHeartbeat;
  private ExecutorService _executor;

  
  /***
   * 
   * @param op
   */
  public Heartbeat(Operation op, long tickInterval, ExecutorService exec) {
    this(op, exec);
    _tickInterval = tickInterval;
  }
  
  
  /***
   * 
   * @param op
   */
  public Heartbeat(Operation op, ExecutorService exec) {
    _op = op;
    _log = new Log4jWrapper(Heartbeat.class, _op);
    _tickInterval = DEFAULT_HEARTBEAT_INTERVAL_MS;
    _executor = exec;
  }
  
  
  /**
   * @throws HeartbeatException *
   * 
   */
  public void start() {
    
    // Sanity check
    _log.debug("Starting heartbeat...");
    if (_timer != null) throw new RuntimeException("timer already exists");
    if (_errorTimer != null) throw new RuntimeException("error timer already exists");
    if (_tickInterval >= HEARTBEAT_KILL_MS) throw new IllegalStateException("tick interval cannot be larger than kill interval");
    
    // Start polling...
    _timer = Utils.timerDedicated(_tickInterval, new Runnable() {
      @Override
      public void run() {
        try {
          tick();
        } catch(Throwable t) {
          _log.error("uncaught heartbeat exception: " + t);
        }
      }
    });
    
    _errorTimer = Utils.timerDedicated(_tickInterval, new Runnable() {
      @Override
      public void run() {
        errorTick();
      }
    });
    
  }
  
  
  /***
   * 
   */
  public void shutdown() {
    _log.info(_op.instanceName() + " Shutting down heartbeat...");
    _errorTimer.cancel(true);
    _heavyHeartbeat.cancel(true);
    _timer.cancel(true);
  }
  
  
  /***
   * 
   * @return
   * @throws HeartbeatException 
   */
  protected String getOperationState() {
    return _op.getState();
  }
  
  
  
  /***
   * 
   * @return
   */
  public Exception maybeGetHeartbeatException() {
    if (this._timer.isDone() || this._timer.isCancelled()) {
      this._unhandledException = new RuntimeException("Internal heartbeat timer is done/cancelled");
      MeteredLog.info(_log, _op.instanceName() + " Heartbeat error: " + this._unhandledException);
      return this._unhandledException; 
    }
    if (this._unhandledException != null) {
      // We encountered an exception... 
      _log.error(_op.instanceName() + " Heartbeat error: " + this._unhandledException);
      return this._unhandledException;
    } else { 
      // We've missed the heartbeat deadline ourselves?
      if (_lastHeartbeat + HEARTBEAT_KILL_MS < System.currentTimeMillis()) {
        this._unhandledException = new RuntimeException("Internal heartbeat miss");
        _log.error(_op.instanceName() + " Heartbeat error: " + this._unhandledException);
        return this._unhandledException; 
      }
    }
    return null;
  }
  
  
  
  /**
   * @throws HeartbeatException 
   * @throws InterruptedException *
   * 
   */
  public synchronized void tick() {

    try {
      
      // Init 
      _ticks++;
      _lastHeartbeat = System.currentTimeMillis();

      handleHeartbeat();
      
      // Maybe exeute a heavy-heartbeat, which is basically chunks of code that can potentially
      // take a long time, so we don't block the main heartbeat. 
      if (_heavyHeartbeat == null || _heavyHeartbeat.isDone()) {
        if (_heavyHeartbeat != null) _heavyHeartbeat.get();  // propagate exceptions
        _heavyHeartbeat = _executor.submit(new Callable<Void>() {
          @Override
          public Void call() throws Exception {

            handleActivityCheck();
            _op.handleStats_ThreadUnsafe();
            _op.handleCoordination_ThreadUnsafe();
            _op.handlePostHeartbeat_ThreadUnsafe();
            
            return null;
          }
        });
      } else {
        _log.info("skipping heavy heartbeat because it's still executing...");
      }
      
    } catch(Exception e) {
      
      // Inform user...
      e.printStackTrace();
      _log.error("Ironic heartbeat error: " + e);
      _op.logger().error(e.getMessage());
      
      // Propagate error back to the operation... This will get rethrown on next iteration...
      _unhandledException = e;
      
    } 
    
  }


  private void debug(String string) {
    // System.err.println(this._op.instanceName() + ": " + string);
  }


  public synchronized void errorTick() {

      // Init 
    String state;
    try {
      state = getOperationState();

      switch (state) {
      case "ERROR": // fall through
      case "KILLED": // fall through
      case "KILLING": // fall through
        return;
      default:
        _op.heartbeatErrorCheck_ThreadUnsafe();
      }
    } catch (Exception e) {
      _log.warn("heartbeat exception in heartbeat error checking thread " + e);
    }

  }
  
  /**
   * @throws HeartbeatException *
   * 
   */
  private void handleActivityCheck() {
    _op.handleActivityCheck_ThreadUnsafe();
  }


  /**
   * @throws HeartbeatException **
   * 
   */
  protected void handleHeartbeat() {
    _op.sendMessageToFlow_ThreadUnsafe("state", getOperationState());
  }
  


  
  /***
   * 
   * @param operation
   * @return
   * @throws HeartbeatException 
   */
  public static Heartbeat create(Operation operation, ExecutorService exec) {
    Heartbeat hb = new Heartbeat(operation, exec);
    hb.start();
    return hb;
  }


  /***
   * 
   * @return
   */
  public boolean isRunning() {
    return _timer != null && !_timer.isDone();
  }


  public long getTicks() {
    return _ticks;
  }


  public long getLastHeartbeat() {
    return _lastHeartbeat;
  }


  
}
