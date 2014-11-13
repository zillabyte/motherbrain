package com.zillabyte.motherbrain.top;

import java.io.IOException;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;
import org.eclipse.jdt.annotation.NonNull;

import com.zillabyte.motherbrain.coordination.CoordinationException;
import com.zillabyte.motherbrain.coordination.MessageHandler;
import com.zillabyte.motherbrain.flow.App;
import com.zillabyte.motherbrain.flow.Component;
import com.zillabyte.motherbrain.flow.FlowException;
import com.zillabyte.motherbrain.flow.FlowInstance;
import com.zillabyte.motherbrain.flow.FlowRecoveryException;
import com.zillabyte.motherbrain.flow.FlowState;
import com.zillabyte.motherbrain.flow.StateMachineException;
import com.zillabyte.motherbrain.flow.config.FlowConfig;
import com.zillabyte.motherbrain.flow.operations.OperationLogger;
import com.zillabyte.motherbrain.flow.rpc.RPCHelper;
import com.zillabyte.motherbrain.universe.ExceptionHandler;
import com.zillabyte.motherbrain.universe.Universe;
import com.zillabyte.motherbrain.utils.JarCompilationException;
import com.zillabyte.motherbrain.utils.Utils;

public class LocalServiceMain {

  static Universe _universe = null;
  static final Logger log = Utils.getLogger(LocalServiceMain.class);
  static FlowInstance _flowInstance = null;
  static final ConcurrentHashMap<String, ReentrantLock> _locks = new ConcurrentHashMap<>();
 

  public static final long INIT_TIMEOUT = 5000;
  public static final long LOCK_TIMEOUT_SECONDS = 15;
  public static final String LOCK_ERROR_RETURN_STRING = "{\"status\": \"error\", \"error_code\": \"lock_timeout\", \"error_message\": \"The app is currently locked by another process.\"}";
  public static final String INTERRUPT_ERROR_RETURN_STRING = "{\"status\": \"error\", \"error_code\": \"interrupt\"}";
  public static final @NonNull FlowState[] nonStartingFlowStates;
  static {
    final EnumSet<FlowState> nonStartingFlowStateSet = EnumSet.complementOf(EnumSet.of(FlowState.INITIAL, FlowState.STARTING));
    final FlowState[] nonStartingFlowStateArray = nonStartingFlowStateSet.toArray(new FlowState[] {});
    /*
     * Contract of Collection#toArray
     */
    assert (nonStartingFlowStateArray != null);
    nonStartingFlowStates = nonStartingFlowStateArray;
  }

  public static final long OPERATION_STARTING_TIMEOUT_MS = TimeUnit.MILLISECONDS.convert(30,  TimeUnit.MINUTES);
  public static final long OPERATION_STARTED_TIMEOUT = TimeUnit.MILLISECONDS.convert(30, TimeUnit.MINUTES);
  public static final long REGISTER_TIMEOUT = OPERATION_STARTED_TIMEOUT;

  /***
   * 
   * @param uni
   * @param fetcher
   */
  public LocalServiceMain() {
    _universe = Universe.instance();
  }



  /**
   * @throws InterruptedException 
   * @throws CoordinationException 
   * @throws SQLException 
   * @throws JarCompilationException 
   * 
   */
  public static void init() throws Exception {

    Universe.instance().topService().init();

  }

  /******************************************************************************/
  /** Helpers ******************************************************************/
  /******************************************************************************/


  /****
   * This method is called whenver API gets an RPC request.  If the RPC is already running, then do nothing. Otherwise, spawn it up. 
   * 
   * @param flowId
   * @param authToken
   * @return
   * @throws InterruptedException
   * @throws StateMachineException 
   */
  private static String handleStartingRPC(final String flowId, final FlowConfig flowConfig) throws InterruptedException {

    // Init, Sanity
    if (flowId == null) {
      throw new NullPointerException("flowId cannot be null!");
    }
    final OperationLogger flowLogger = Universe.instance().loggerFactory().logger(flowId, "component");      

    // Prepare a timeout because we don't want the lock to never expire!
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<String> future = executor.submit(new Callable<String>() {

      @Override
      public String call() throws Exception {

        // Acquire a lock
        final ReentrantLock lock = getLock(flowId);
        try {
          if (!lock.tryLock(LOCK_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
            log.info("Component currently locked by another process");
            return LOCK_ERROR_RETURN_STRING;
          }
        } catch (InterruptedException e) {
          flowLogger.error("Interrupted!");
          return INTERRUPT_ERROR_RETURN_STRING;
        }
        try {
          // Init
          log.info("start_rpc: " + flowId);
          flowLogger.writeLog("Beginning RPC", OperationLogger.LogPriority.STARTUP);

          // Get the flow
          App flow = (App)
              RPCHelper.wrapComponentInRpcApp(
                  (Component) Universe.instance().flowBuilderFactory()
                  .createFlowBuilder(flowConfig, flowLogger)
                  .buildFlow(flowId)
                  );


          // Start running the flow... 
          FlowInstance flowInstance = Universe.instance().topService().registerApp(flow);
          log.info("Flow instance name: " + flowInstance.id() + "__" + flowInstance.name());
          _flowInstance = flowInstance;
          flowLogger.writeLog("RPC deploying to cluster.", OperationLogger.LogPriority.STARTUP);
          /*
           * We unlock here because from here on we don't really write, only read.
           */
          lock.unlock();
          /*
           * Wait for the operations to come online...
           */
          flowLogger.writeLog("Waiting for operations to come online: " + flowInstance.getOperationsThatAreNotAlive(), OperationLogger.LogPriority.STARTUP);
          if (!flowInstance.waitUntilAllOperationsAlive(LocalServiceMain.OPERATION_STARTING_TIMEOUT_MS, FlowState.INITIAL, FlowState.STARTING, FlowState.STARTED, FlowState.RUNNING)) {
            if (flowInstance.inState(FlowState.INITIAL, FlowState.STARTING, FlowState.STARTED, FlowState.RUNNING) == false) {
              flowLogger.writeLog("Component in unexpected state " + flowInstance.getFlowState() + ".  Aborting...", OperationLogger.LogPriority.STARTUP);
              return "{\"status\": \"error\", \"error_message\": \"unexpected state " + flowInstance.getFlowState() + " \"}";
            }
            flowLogger.writeLog("Operation boot timeout", OperationLogger.LogPriority.STARTUP);
            flowInstance.transitionToState(FlowState.ERROR);
            return "{\"status\": \"error\", \"error_message\": \"operation boot timeout\"}";
          }
          flowLogger.writeLog("Operations online.", OperationLogger.LogPriority.STARTUP);
          flowLogger.writeLog("Beginning individual operation initialization.", OperationLogger.LogPriority.STARTUP);
          /*
           * Wait for all the operations to report they are ready to start working...
           */
          if (!flowInstance.waitForState(OPERATION_STARTED_TIMEOUT, LocalServiceMain.nonStartingFlowStates)) {
            flowLogger.writeLog("Operation prepare timeout", OperationLogger.LogPriority.STARTUP);
            flowInstance.transitionToState(FlowState.ERROR);
            return "{\"status\": \"error\", \"error_message\": \"operation prepare timeout\"}";
          }
          if (flowInstance.inState(FlowState.STARTED, FlowState.RUNNING) == false) {
            flowLogger.writeLog("Component in unexpected state " + flowInstance.getFlowState() + ". Aborting...", OperationLogger.LogPriority.STARTUP);
            return "{\"status\": \"error\", \"error_message\": \"unexpected state " + flowInstance.getFlowState() + " \"}";
          }

          // Done!
          flowLogger.writeLog("RPC deployed.", OperationLogger.LogPriority.STARTUP);

        } catch (InterruptedException e) {
          flowLogger.error("Interrupted!");
          return INTERRUPT_ERROR_RETURN_STRING;
        } catch(MotherbrainException ex) {
          flowLogger.writeLog(ex.getInternalMessage(), OperationLogger.LogPriority.ERROR);
          throw ex;
        } finally {
          /*
           * Unlock if someone else didn't grab the lock.
           */
          while (lock.getHoldCount() > 0) {
            lock.unlock();
          }
        }
        return "{\"status\": \"success\"}";
      }
    });


    // get the result before timeout
    try {

      // Success!
      return future.get(REGISTER_TIMEOUT, TimeUnit.MILLISECONDS);

    } catch (TimeoutException e) {
      log.info("flow register timeout");
      return "{\"status\": \"error\", \"error_message\": \"flow register timeout\"}}";
    } catch (ExecutionException e) {
      log.info("execution exception");
      e.printStackTrace();
      return "{\"status\": \"error\", \"error_message\": \"execution exception\"}}";
    } finally {
      executor.shutdownNow();
    }

  }

  private static String handleRegisteringApp(final FlowConfig flowConfig) throws InterruptedException {

    // Init 
    // Create the logger... 
    final OperationLogger flowLogger = Universe.instance().loggerFactory().logger(flowConfig.getFlowId(), "app");

    // Prepare a timeout because we don't want the lock to never expire!
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<String> future = executor.submit(new Callable<String>() {

      @Override
      public String call() throws Exception {
        if (flowConfig.getFlowId() == null) {
          throw new NullPointerException(flowConfig.getFlowId());
        }

        // Acquire a lock
        final ReentrantLock lock = getLock(flowConfig.getFlowId());
        try {
          if (!lock.tryLock(LOCK_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
            log.info("App currently locked by another process");
            return LOCK_ERROR_RETURN_STRING;
          }
        } catch (InterruptedException e) {
          flowLogger.error("Interrupted!");
          return INTERRUPT_ERROR_RETURN_STRING;
        }
        try {
          // Init
          log.info("register_flow: " + flowConfig.getFlowId() + " v" + flowConfig.getFlowVersion());
          flowLogger.writeLog("Beginning app deployment", OperationLogger.LogPriority.STARTUP);

          // Get the flow
          App flow = (App)Universe.instance().flowBuilderFactory()
              .createFlowBuilder(flowConfig, flowLogger)
              .buildFlow(flowConfig.getFlowId());

          // Start running the flow... 
          FlowInstance flowInstance = Universe.instance().topService().registerApp(flow);
          log.info("Flow instance id: " + flowInstance.id() + " version: " + flowInstance.version() + " name:" + flowInstance.name());

          _flowInstance = flowInstance;
          flowLogger.writeLog("App deploying to cluster.", OperationLogger.LogPriority.STARTUP);

          /*
           * We unlock here because from here on we don't really write, only read.
           */
          lock.unlock();
          /*
           * Wait for the operations to come online...
           */
          flowLogger.writeLog("Waiting for operations to come online: " + flowInstance.getOperationsThatAreNotAlive(), OperationLogger.LogPriority.STARTUP);
          if (!flowInstance.waitUntilAllOperationsAlive(LocalServiceMain.OPERATION_STARTING_TIMEOUT_MS, FlowState.INITIAL, FlowState.STARTING, FlowState.STARTED, FlowState.RUNNING)) {
            if (flowInstance.inState(FlowState.INITIAL, FlowState.STARTING, FlowState.STARTED, FlowState.RUNNING) == false) {
              flowLogger.writeLog("App in unexpected state " + flowInstance.getFlowState() + ".  Aborting...", OperationLogger.LogPriority.STARTUP);
              return "{\"status\": \"error\", \"error_message\": \"unexpected state " + flowInstance.getFlowState() + " \"}";
            }
            flowLogger.writeLog("Operation boot timeout", OperationLogger.LogPriority.STARTUP);
            flowInstance.transitionToState(FlowState.ERROR);
            return "{\"status\": \"error\", \"error_message\": \"operation boot timeout\"}";
          }
          flowLogger.writeLog("Operations online.", OperationLogger.LogPriority.STARTUP);
          flowLogger.writeLog("Beginning individual operation initialization.", OperationLogger.LogPriority.STARTUP);
          /*
           * Wait for all the operations to report they are ready to start working...
           */
          if (!flowInstance.waitForState(OPERATION_STARTED_TIMEOUT, LocalServiceMain.nonStartingFlowStates)) {
            flowLogger.writeLog("Operation prepare timeout", OperationLogger.LogPriority.STARTUP);
            flowInstance.transitionToState(FlowState.ERROR);
            return "{\"status\": \"error\", \"error_message\": \"operation prepare timeout\"}";
          }
          if (flowInstance.inState(FlowState.STARTED, FlowState.RUNNING) == false) {
            flowLogger.writeLog("App in unexpected state " + flowInstance.getFlowState() + ". Aborting...", OperationLogger.LogPriority.STARTUP);
            return "{\"status\": \"error\", \"error_message\": \"unexpected state " + flowInstance.getFlowState() + " \"}";
          }
          // Done!
          flowLogger.writeLog("App deployed.", OperationLogger.LogPriority.STARTUP);
        } catch (InterruptedException e) {
          flowLogger.error("Interrupted!");
          return INTERRUPT_ERROR_RETURN_STRING;
        } catch(MotherbrainException ex) {
          flowLogger.writeLog(ex.getInternalMessage(), OperationLogger.LogPriority.ERROR);
          throw ex;
        } finally {
          /*
           * Unlock if someone else didn't grab the lock.
           */
          while (lock.getHoldCount() > 0) {
            lock.unlock();
          }
        }
        return "{\"status\": \"success\"}";
      }
    });

    // get the result before timeout
    try {
      // Success!
      return future.get(REGISTER_TIMEOUT, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      log.info("flow register timeout");
      return "{\"status\": \"error\", \"error_message\": \"flow register timeout\"}}";
    } catch (ExecutionException e) {
      log.info("execution exception");
      e.printStackTrace();
      flowLogger.error(MotherbrainException.getRootUserMessage(e, "Internal cluster error"));
      return "{\"status\": \"error\", \"error_message\": \"execution exception\"}}";
    } finally {
      executor.shutdownNow();
    }
  }





  /******************************************************************************/
  /** Aux ***********************************************************************/
  /******************************************************************************/




  /***
   * 
   * @param flowId
   */
  static ReentrantLock getLock(String flowId) {
    _locks.putIfAbsent(flowId, new ReentrantLock(true));
    final ReentrantLock lock = _locks.get(flowId);
    return lock;
  }




  /***
   * 
   * @param id
   */
  public static FlowInstance getFlowInstance(String id) {
    return _flowInstance;
  }



  /******************************************************************************/
  /** Main **********************************************************************/
  /**
   * @throws ParseException
   * @throws IOException 
   * @throws JarCompilationException 
   * @throws FlowException 
   * @throws FlowRecoveryException 
   * @throws SQLException  
   * @throws InterruptedException 
   * @throws StateMachineException 
   * @throws ExecutionException 
   * @throws CoordinationException 
   * @throws IllegalArgumentException 
   */



  public static void main(String[] args) throws ParseException, JarCompilationException, SQLException, FlowRecoveryException, FlowException, IOException, InterruptedException, StateMachineException, CoordinationException, ExecutionException {

    // Parse arguments
    LocalCommandLineHelper.printObligatoryCoolBanner();
    LocalCommandLineHelper.createUniverse(args);

    // Init the server...
    try {
      Utils.executeWithin(INIT_TIMEOUT, new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          init();
          return null;
        }
      });
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(0);
    }

    String flowName = Universe.instance().config().getOrException("flow.name");
    if(Universe.instance().config().getOrException("flow.class").equals("app")) {
      handleRegisteringApp(FlowConfig.createMock(flowName, 0, 0) );
    } else {
      handleStartingRPC(flowName, FlowConfig.createMock(flowName, 0, 0));
    }
    _flowInstance.startNewCycle();
    _flowInstance.waitForState(FlowState.WAITING_FOR_NEXT_CYCLE, FlowState.IDLE);
    _flowInstance.kill();
    clear();
    System.exit(0);

  }

  public static void clear() {
    _flowInstance = null;
    _locks.clear();
  }

}
