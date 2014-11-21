package com.zillabyte.motherbrain.flow.operations.multilang.builder;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;

import net.sf.json.JSONObject;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.log4j.Logger;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Maps;
import com.google.common.collect.Multiset;
import com.google.monitoring.runtime.instrumentation.common.com.google.common.collect.Iterators;
import com.zillabyte.motherbrain.benchmarking.Benchmark;
import com.zillabyte.motherbrain.container.ContainerEnvironmentHelper;
import com.zillabyte.motherbrain.container.ContainerException;
import com.zillabyte.motherbrain.container.ContainerFactory;
import com.zillabyte.motherbrain.container.ContainerWrapper;
import com.zillabyte.motherbrain.flow.App;
import com.zillabyte.motherbrain.flow.Component;
import com.zillabyte.motherbrain.flow.Flow;
import com.zillabyte.motherbrain.flow.FlowCompilationException;
import com.zillabyte.motherbrain.flow.buffer.SinkToBuffer;
import com.zillabyte.motherbrain.flow.buffer.SourceFromBuffer;
import com.zillabyte.motherbrain.flow.components.ComponentInput;
import com.zillabyte.motherbrain.flow.components.ComponentOutput;
import com.zillabyte.motherbrain.flow.components.builtin.BuiltinComponents;
import com.zillabyte.motherbrain.flow.config.FlowConfig;
import com.zillabyte.motherbrain.flow.graph.Connection;
import com.zillabyte.motherbrain.flow.graph.FlowGraph;
import com.zillabyte.motherbrain.flow.operations.Join;
import com.zillabyte.motherbrain.flow.operations.Operation;
import com.zillabyte.motherbrain.flow.operations.OperationLogger;
import com.zillabyte.motherbrain.flow.operations.builtin.Count;
import com.zillabyte.motherbrain.flow.operations.builtin.RateLimiter;
import com.zillabyte.motherbrain.flow.operations.builtin.Unique;
import com.zillabyte.motherbrain.flow.operations.decorators.EmitDecorator;
import com.zillabyte.motherbrain.flow.operations.decorators.RemoveFields;
import com.zillabyte.motherbrain.flow.operations.decorators.RenameFields;
import com.zillabyte.motherbrain.flow.operations.decorators.RetainFields;
import com.zillabyte.motherbrain.flow.operations.multilang.MultiLangProcess;
import com.zillabyte.motherbrain.flow.operations.multilang.MultiLangProcessException;
import com.zillabyte.motherbrain.flow.operations.multilang.operations.LocalComponent;
import com.zillabyte.motherbrain.flow.operations.multilang.operations.MultiLangAggregator;
import com.zillabyte.motherbrain.flow.operations.multilang.operations.MultiLangRunEach;
import com.zillabyte.motherbrain.flow.operations.multilang.operations.MultiLangRunSource;
import com.zillabyte.motherbrain.universe.Config;
import com.zillabyte.motherbrain.universe.Universe;
import com.zillabyte.motherbrain.utils.JSONUtil;
import com.zillabyte.motherbrain.utils.Utils;
import com.zillabyte.motherbrain.utils.VersionComparer;

@SuppressWarnings("unchecked")
public class MultilangFlowCompiler {

  public final Long CLI_INFO_TIMEOUT = Config.getOrDefault("builder.cli.info.timeout", 1000L * 15);
  public final Long CLI_PREP_TIMEOUT = Config.getOrDefault("builder.cli.prep.timeout", 1000L * 60 * 5);
  public final String MINIMUM_REQUIRED_VERSION = Config.getOrDefault("builder.cli.min.version", "0.9.24");
  public final static int MAX_SLOTS_PER_FLOW = Config.getOrDefault("builder.max.slots.per.flow", 30);
  private final static Logger _log = Logger.getLogger(MultilangFlowCompiler.class);

  private FlowFetcher _fetcher = null;
  private FlowValidator _validator = new FlowValidator();
  private ContainerFactory _containerFactory = Universe.instance().containerFactory();
  private FlowConfig _flowConfig;
  private ContainerWrapper _container;
  private OperationLogger _logger;
  private Map<String, MutableInt> _prefixes = Maps.newHashMap();
  private Multiset<String> _componentCounts = HashMultiset.create();
  
  
  /***
   * 
   */
  public MultilangFlowCompiler(FlowFetcher fetcher, FlowConfig baseFlowConfig, ContainerWrapper container, OperationLogger logger) {
    _fetcher = fetcher;
    _flowConfig = baseFlowConfig;
    _container = container;
    _logger = logger;
  }

  
  /***
   * 
   * @param flowId
   * @param overrideConfig 
   * @param logger
   * @param flowConfig
   * @return
   * @throws FlowCompilationException
   * @throws ContainerException
   */
  public Flow compileFlow(String flowId, JSONObject overrideConfig) throws FlowCompilationException, ContainerException {
    
    // Step 1: run "zillabyte prep"
    if(Universe.instance().env().isTestOrProd()) handlePrep(flowId, overrideConfig);
    
    // Step 2: run "zillabyte info" and parse the settings 
    JSONObject zbInfo = handleGettingSettings(flowId);
    if (zbInfo == null) {
      throw (FlowCompilationException) new FlowCompilationException().setAllMessages("Unable to retrieve 'zillabyte info' response.").adviseRetry();
    }
    
    // Step 3: Build a flow from the settings... 
    Flow flow = buildFlowFromSettings(flowId, zbInfo, overrideConfig);
    
    // Save the settings for later
    flow.setMeta(zbInfo);
    
    return flow;
  }



  /****
   * 
   * @param overrideConfig 
   * @param logger 
   * @param settings
   * @return
   * @throws FlowCompilationException 
   */
  protected Flow buildFlowFromSettings(String flowId, JSONObject zbInfo, JSONObject overrideConfig) throws FlowCompilationException {
    
    // Init
    String flowType = zbInfo.optString("flow_type", "app");
    String name = zbInfo.optString("name", flowId);
    
    handleEarlySanityChecks(zbInfo);
    
    // Build the base flow 
    Flow flow = null;
    if (flowType.equalsIgnoreCase("app")) {
      flow = new App(flowId, name, (FlowConfig)_flowConfig.mergeWith(overrideConfig));
    } else {
      flow = new Component(flowId, name, (FlowConfig)_flowConfig.mergeWith(overrideConfig));
    }
    flow.setLogger(this._logger);
    
    // Build the operations
    Map<String, Object> operationMap = Maps.newHashMap();
    
    _log.info("zbInfo contains " + zbInfo.toString());
    _log.info("overrideConfig contains " + overrideConfig.toString());


    // Update node configurations
    if (overrideConfig.containsKey("nodes")){
      zbInfo.put("nodes", overrideConfig.get("nodes"));
    }
    
    for (JSONObject nodeSettings : (List<JSONObject>)zbInfo.getJSONArray("nodes")) {
      Object operation = createOperationFromJSON(flowId, nodeSettings);
      operationMap.put(nodeSettings.getString("name"), operation);
    }
    
    // Set the container flow
    for(Object o : operationMap.values()) {
      if (o instanceof Operation) {
        ((Operation)o).setContainerFlow(flow);
      } else if (o instanceof Flow) {
        ((Flow) o).setParentFlow(flow);
      }
    }
    
    // Now add the connections...  
    FlowGraph flowGraph = flow.graph();
    for(JSONObject arc : (List<JSONObject>)zbInfo.getJSONArray("arcs")) {
      
      // INIT 
      String originName = arc.optString("origin");
      String destName = arc.optString("dest");
      String arcName = arc.optString("name");
      Boolean loopBack = arc.containsKey("loop_back") ? true : false;
      Integer maxIter = arc.optInt("max_iterations", Connection.DEFAULT_MAX_ITER);
      
      // Sanity 
      if (originName == null || operationMap.containsKey(originName) == false) 
        throw (FlowCompilationException) new FlowCompilationException().setAllMessages("Could not find operation with name: '" + originName + "'");
      if (destName == null || operationMap.containsKey(destName) == false) 
        throw (FlowCompilationException) new FlowCompilationException().setAllMessages("Could not find operation with name: '" + destName + "'");
      if (arcName == null)
        throw (FlowCompilationException) new FlowCompilationException().setAllMessages("Connection did not have a name: '" + arc + "'");
      if (loopBack) {
        Object loopBackNode = operationMap.get(destName);
        if(loopBackNode instanceof Operation) {
          if( ((Operation) loopBackNode).type().equalsIgnoreCase("source") ) throw (FlowCompilationException) new FlowCompilationException().setAllMessages("Cannot loop back to a source");
        }
      }
      
      // Add to the graph...
      Object origin = operationMap.get(originName);
      Object dest = operationMap.get(destName);
      
      // Connect the nodes..
      handleConnectingNodes(origin, dest, arcName, loopBack, maxIter, flowGraph);
        
    }
    
    
    // Remove all placeholders... 
    boolean converged; 
    do {
      converged = true;
      for(PlaceHolderOperation ph : flowGraph.getByType(PlaceHolderOperation.class)) {
        if (ph.getObject() instanceof EmitDecorator) {
          
          EmitDecorator ed = (EmitDecorator)ph.getObject();
          Connection conn = Iterators.getOnlyElement(flowGraph.connectionsTo(ph).iterator());
          Operation origin = conn.source();
          
          if (origin instanceof PlaceHolderOperation == false) {
            
            // This is a real operation...
            origin.addEmitDecorator(conn.streamName(), ed);
            flowGraph.pluck(ph);
            
          } else {
            
            // Otherwise, continue iterating, because we have not converged yet. 
            converged = false;
            
          }
          
        } else if (ph.getObject() instanceof RouteBy) {
          
          RouteBy rb = (RouteBy)ph.getObject();
          Operation origin = Iterators.getOnlyElement(flowGraph.operationsTo(ph).iterator());
          Operation dest = Iterators.getOnlyElement(flowGraph.operationsFrom(ph).iterator());
          
          flowGraph.pluck(ph);
          dest.setIncomingRouteByFields(rb.getFields());
          
        } else { 
          throw new IllegalStateException();
        }
      }
    } while(!converged);
    
    
    // Set parallelism
    setParallelism(flow);
    
    // Validate... 
    _validator.validate(flow);
    
    // Done 
    return flow;
  }


  
  
  /***
   * 
   * @param zbInfo
   * @throws FlowCompilationException
   */
  private void handleEarlySanityChecks(JSONObject zbInfo) throws FlowCompilationException {
    
    // Multilang version...  
    String multilangVersion = zbInfo.optString("multilang_version", "0.0.0");
    if (VersionComparer.isAtLeast(multilangVersion, MINIMUM_REQUIRED_VERSION) == false) {
      throw (FlowCompilationException) new FlowCompilationException().setAllMessages("The flow is built with an older version of Zillabyte (" + multilangVersion + ") . Please upgrade dependencies.");
    }
    
  }


  
  /****
   * 
   * @param flow
   * @throws FlowCompilationException 
   */
  private void setParallelism(Flow flow) throws FlowCompilationException {

    final int actualNodes = flow.getExpectedNumberOfNodes();
    _log.info("There are " + actualNodes + " nodes expected for this flow. (" + flow.getId() + ")");
    
    if (actualNodes == 0) {
      throw (FlowCompilationException) new FlowCompilationException().setAllMessages("The flow has no nodes");
    }
    
    // Each operation must have parallelism of at least 1.
    int idealSlotsPerOperation = Math.max(MAX_SLOTS_PER_FLOW / (actualNodes), 1);
    
    Collection<Operation> operations = flow.getOperations();
    for (Operation o : operations) {
      // Set a default target parallelism of atleast 1 if we have not already set it
      if (!o.getParallelismOverriden()){
        o.setTargetParallelism(idealSlotsPerOperation);
      }
      o.setTargetParallelism(Math.min(o.getTargetParallelism(), o.getMaxParallelism()));
    }
  }
  


  /****
   * 
   * @param origin
   * @param dest
   * @param arcName
   * @param flowGraph
   * @throws FlowCompilationException
   */
  protected void handleConnectingNodes(Object origin, Object dest, String arcName, Boolean loopBack, Integer maxIter, FlowGraph flowGraph) throws FlowCompilationException {
    
    if (origin instanceof Operation && dest instanceof Operation) {
      
      // Case: operation -> operation
      flowGraph.connect((Operation)origin, (Operation)dest, arcName, loopBack, maxIter);
      
    } else if (origin instanceof Operation && dest instanceof Component) {
      
      // Case: operation -> component
      Component c = (Component)dest;
      Operation o = (Operation)origin;
      
      maybeInjectComponent(flowGraph, c);
      flowGraph.connect(o, c.getOneInput(), arcName, loopBack, maxIter);
      
    } else if (origin instanceof Component && dest instanceof Component) {
      
      // Case: component -> component
      Component o = (Component)origin;
      Component d = (Component)dest;
      
      maybeInjectComponent(flowGraph, d);
      flowGraph.connect(o.getOneOutput(), d.getOneInput(), arcName, loopBack, maxIter);
      
    } else if (origin instanceof Component && dest instanceof Operation) {
      
      // Case: component -> operation
      Component c = (Component)origin;
      Operation d = (Operation)dest;
      
      maybeInjectComponent(flowGraph, c);
      flowGraph.connect(c.getOneOutput(), d, arcName, loopBack, maxIter);
      
    } else {
      
      throw (FlowCompilationException) new FlowCompilationException().setAllMessages("Unknown graph case. The origin is a "+origin.getClass().getName()+" while the destination is a "+dest.getClass().getName()+".");
      
    }
  }

  
  
  private void maybeInjectComponent(FlowGraph flowGraph, Component c) throws FlowCompilationException {
    
    // Only inject if it hasn't already been done above (this handles the source-from-component case)
    if (flowGraph.containsAny(c.graph().allOperations()) == false) {
      String prefix = c.getName() + "." + _componentCounts.count(c.getId());
      flowGraph.inject(prefix, c.graph());
      c.setGraph(flowGraph);
      _componentCounts.add(c.getId());
    }
  }
  
  
  
  /****
   * 
   * @param c
   * @return
   */
  private String getPrefixFor(Component c) {
    if (_prefixes.containsKey(c.getName()) == false) {
      _prefixes.put(c.getName(), new MutableInt(1));
      return c.getName();
    } else {
      _prefixes.get(c.getName()).increment();
      return c.getName() + "-" + _prefixes.get(c.getName()).intValue();
    }
  }


  /***
   * 
   * @param container
   * @throws FlowCompilationException 
   * @throws ContainerException 
   */
  protected JSONObject handleGettingSettings(final String flowId) throws FlowCompilationException, ContainerException {

    // INIT 
    Benchmark.markBegin("multilang.container.zillabyte_info");
    
    try { 
      
      // The retry here is mostly for tests, which can have hiccups when run in parallel... 
      return Utils.retry(new Callable<JSONObject>() {
        
        @Override
        public JSONObject call() throws Exception {
          try { 
              
            // Execute the command...
            MultiLangProcess proc = _container.buildCommand()
                .withEnvironment(ContainerEnvironmentHelper.getCLIEnvironment(_flowConfig))
                .withCLICommand("info")
                .withSockets()
                .inFlowDirectory(flowId)
                .createProcess()
                .addLogListener(_logger)
                .addStdioLogListeners()
                .start();
            
            String message = proc.getNextMessage(CLI_INFO_TIMEOUT);
            proc.waitForExit(CLI_INFO_TIMEOUT);
            
            // Parse the results, finish.. 
            return JSONUtil.parseObj(message);
            
          } catch (TimeoutException ex) {
            throw (FlowCompilationException)new FlowCompilationException(ex).setAllMessages("Timeout retrieving flow meta information.").adviseRetry();
          } catch (ContainerException e) {
            throw (FlowCompilationException)new FlowCompilationException(e).setAllMessages("Error initializing flow container.").adviseRetry();
          } catch (InterruptedException e) {
            throw (FlowCompilationException)new FlowCompilationException(e).setAllMessages("Flow compilation interrupted.").adviseRetry();
          } catch (MultiLangProcessException e) {
            throw new FlowCompilationException(e);
          }
        }
      });
      
    } catch(Exception e) {
      throw new FlowCompilationException(e);
    } finally {
      Benchmark.markEnd("multilang.container.zillabyte_info");
    }
      
  }


  /***
   * 
   * @param overrideConfig 
   * @param container
   * @param flowConfig
   * @throws FlowCompilationException 
   */
  protected void handlePrep(String flowId, Map<String, Object> overrideConfig) throws FlowCompilationException {
    try { 
      
      // Benchmark
      Benchmark.markBegin("multilang.container.zillabyte_prep");
      
      // Execute 
      _container.buildCommand()
          .withEnvironment(ContainerEnvironmentHelper.getCLIEnvironment(this._flowConfig))
          .withEnvironment("ZILLABYTE_PARAMS", _flowConfig.mergeWith(overrideConfig).toJSON().toString())
          .withCLICommand("prep", "--mode", Universe.instance().env().toString())
          .inFlowDirectory(flowId)
          .withoutSockets()
          .createProcess()
          .addLogListener(_logger)
          .start()
          .waitForExit();
          
    } catch (InterruptedException e) {
      throw (FlowCompilationException)new FlowCompilationException(e).setUserMessage("Interrupted");
    } catch (ContainerException | MultiLangProcessException e) {
      throw (FlowCompilationException)new FlowCompilationException(e).setUserMessage("Error with app container");
    } finally {
      Benchmark.markEnd("multilang.container.zillabyte_prep");
    }
  }
  
  
  
  

  /***
   * 
   * @param containerFlow 
   * @param flow
   * @param container
   * @param nodeSettings
   * @return
   * @throws FlowCompilationException 
   */
  protected Object createOperationFromJSON(String flowId, JSONObject node) throws FlowCompilationException {
    try { 
       
      // INIT 
      String nodeType = node.getString("type").toLowerCase();
      Operation operation;
  
      // Build the operation
      switch(nodeType) {
      case "source":
        
        if (node.containsKey("relation") || node.containsKey("matches")) {
          // SourceFromRelation has 'relation' or 'matches' 
          String query = node.containsKey("relation") ? node.getJSONObject("relation").getString("query") : node.getString("matches");

          // The one true source.
          return new SourceFromBuffer(node.getString("name"), query, flowId, this._flowConfig.getAuthToken());
          
        } else {        
          // Custom sources don't have a query.. 
          operation = new MultiLangRunSource(node, _container);
        }
        break;
        
      case "each":
      case "filter":
        
        operation = new MultiLangRunEach(node, _container);
        break;

      case "group_by":
        
        operation = new MultiLangAggregator(node, _container);
        break;

      case "rename": 
        
        operation = new PlaceHolderOperation(new RenameFields(node));
        break;

      case "retain": 
        
        operation = new PlaceHolderOperation(new RetainFields(node));
        break;

      case "remove":
        
        operation = new PlaceHolderOperation(new RemoveFields(node));
        break;

      case "unique":
        
        operation = new Unique(node);
        break;

      case "clump":
        throw new NotImplementedException();

      case "count":
        
        operation = new Count(node);
        break;

      case "join":
        
        operation = new Join(node);
        break;

      case "component":
        

        if(Universe.instance().env().isLocal()) {
          // TODO: move this logic to InPlaceFlowBuilder
          if (BuiltinComponents.exists(node.optString("id", ""))) {
            return BuiltinComponents.create(node.optString("id"), FlowConfig.createFromJSON(node.getJSONObject("config")));
          } else {
            return new LocalComponent(node);
          }
        } else {
          
          // Recursively build a new flow... 
          String compName = node.getString("id");
          JSONObject config = new JSONObject();
          if (node.has("config")) {
            config = node.optJSONObject("config");
          }

          _log.info("recursively building flow: " + compName + " with config: " + config);
          Flow subFlow = _fetcher.buildFlow(compName, config);
          if (subFlow instanceof App) 
            throw (FlowCompilationException) new FlowCompilationException().setAllMessages("Only components may be nested.");
          
          return subFlow;
        }
        
      case "sink":
        
        operation = new SinkToBuffer(node, _flowConfig);
        break;
        
      case "input":
              
        operation = new ComponentInput(node, _flowConfig);
        break;

      case "output":
        
        operation = new ComponentOutput(node, _flowConfig);
        break;

      case "route_by":
        
        operation = new PlaceHolderOperation(new RouteBy(node));
        break;

      case "rate_limit":
        
        operation = new RateLimiter(node);
        break;

        
      default: 
        throw (FlowCompilationException) new FlowCompilationException().setAllMessages("Unknown operation type: " + nodeType+".");
      
      }

      // Set Target Parallelism
      if (node.containsKey("parallelism")){
        operation.setTargetParallelism(node.getInt("parallelism"));
        operation.setParallelismOverriden(true);
      }
      
      return operation;
      
    
    } catch(ContainerException | InterruptedException e) {
      throw new FlowCompilationException(e);
    }
  }
  
  
}
