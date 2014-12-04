package com.zillabyte.motherbrain.flow.operations.multilang.builder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeoutException;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.log4j.Logger;

import com.google.common.io.Files;
import com.zillabyte.motherbrain.api.APIException;
import com.zillabyte.motherbrain.api.APIService;
import com.zillabyte.motherbrain.container.ContainerEnvironmentHelper;
import com.zillabyte.motherbrain.container.ContainerException;
import com.zillabyte.motherbrain.container.ContainerPathHelper;
import com.zillabyte.motherbrain.container.ContainerWrapper;
import com.zillabyte.motherbrain.flow.Flow;
import com.zillabyte.motherbrain.flow.FlowCompilationException;
import com.zillabyte.motherbrain.flow.components.builtin.BuiltinComponents;
import com.zillabyte.motherbrain.flow.config.FlowConfig;
import com.zillabyte.motherbrain.flow.operations.OperationLogger;
import com.zillabyte.motherbrain.flow.operations.multilang.MultiLangProcessException;
import com.zillabyte.motherbrain.universe.S3Exception;
import com.zillabyte.motherbrain.universe.Universe;
import com.zillabyte.motherbrain.utils.Utils;
import com.zillabyte.motherbrain.utils.dfs.DFSServiceWrapper;

public class APIFlowBuilder implements FlowFetcher {

  /**
   * 
   */
  private static final long serialVersionUID = 5830540114377373350L;
  
  private static final Logger _log = Utils.getLogger(APIFlowBuilder.class);
  private static final Long CLI_PULL_TIMEOUT = 1000L * 60 * 2;
  private static final long EXPLODED_FILE_COPY_THRESHOLD = 100_000L;  // don't copy files larger than this to the exploded view... 
  
  private MultilangFlowCompiler _flowBuilder;
  private OperationLogger _logger;
  private APIService _api;
  private ContainerWrapper _container;
  private FlowConfig _flowConfig;

  
  
  /***
   * 
   * @param authToken
   */
  public APIFlowBuilder(FlowConfig flowConfig, ContainerWrapper destContainer, OperationLogger logger) {
    _flowBuilder = new MultilangFlowCompiler(this, flowConfig, destContainer, logger);
    _flowConfig = flowConfig;
    _logger = logger;
    _api = Universe.instance().api();
    _container = destContainer;
  }
  
  
  
  
  /**
   * 
   * @param id
   * @param flowLogger
   * @param destDir
   * @throws InterruptedException
   * @throws LXCException
   * @throws FlowCompilationException 
   */
  @Override
  public Flow buildFlow(String flowName, JSONObject overrideConfig) throws FlowCompilationException {
    try {
    
      // Builtin?
      if (BuiltinComponents.exists(flowName)) {
        return BuiltinComponents.create(flowName, overrideConfig);
      }

      // Step 1: ask the API to resolve the ID to something concrete... 
      JSONObject settings = _api.getFlowSettings(flowName, _flowConfig.getAuthToken());
      String concreteId = settings.getString("id");
      Integer version = Integer.valueOf( settings.getString("version") );
      
      // Do we have a cached version of this flow?
      Flow cachedFlow  = null;
      try {
        cachedFlow = _container.maybeGetCachedFlow(concreteId, version);
      } catch(Exception e) {
        _log.warn("unable to deserialize cached flow: " + e.getMessage());
        cachedFlow = null;
      }
      
      if (cachedFlow != null) {
        return cachedFlow;
        
      } else {
        
        // Step 1b: write a file, make sure it's writable. 
        _container.createDirectory(ContainerPathHelper.internalPathForFlow(concreteId));
        
        // Step 2: use the CLI to pull into the container...
        _container.buildCommand()
          .withEnvironment(ContainerEnvironmentHelper.getCLIEnvironment(this._flowConfig))
          .withCLICommand("pull", concreteId, ".")
          .inFlowDirectory(concreteId)
          .withoutSockets()
          // Run it 
          .createProcess()
          .addLogListener(_logger)
          .start()
          .waitForExit(CLI_PULL_TIMEOUT);
        
        // Step 3: Actually build the flow
        Flow flow = _flowBuilder.compileFlow(concreteId, overrideConfig);
        
        // Step 4: Capture all the files are report them back to the API for UI stuff
        flow.addMeta("files", buildMetaFiles(concreteId, version));
        
        // Step 5: Cache it for later use... 
        _container.cacheFlow(flow);
        
        return flow;
        
      }
       
    } catch(InterruptedException | MultiLangProcessException | ContainerException | TimeoutException | APIException | IOException | S3Exception e) {
      throw (FlowCompilationException) new FlowCompilationException(e).setUserMessage("An error occurred while pulling flow "+flowName+" from our servers.").adviseRetry();
    }
  }

  
  public Flow buildFlow(String flowName) throws FlowCompilationException {
    return buildFlow(flowName, new JSONObject());
  }

  
  private JSONArray buildMetaFiles(String concreteId, Integer version) throws ContainerException, IOException, S3Exception {
    
    // INIT
    DFSServiceWrapper dfs = Universe.instance().dfsService();
    File dir = _container.getFlowRoot(concreteId);
    Path rootPath = Paths.get(dir.getAbsolutePath());
    
    // Traverse each file...
    JSONArray fileList = new JSONArray();
    for(File file : Files.fileTreeTraverser().preOrderTraversal(dir)) {
      
      // File?
      if (!file.isFile() || file.getAbsolutePath().contains("/vEnv/")) continue; // Ignore python vEnv
      
      // Init 
      Path path = Paths.get(file.getAbsolutePath());
      Path rel = rootPath.relativize(path);
      String dfsPath = "/flow_files/" + concreteId + "/cycle_" + version + "/" + rel.toString();
      JSONObject info = new JSONObject();

      // Meta hash...
      info.put("file", rel.toString());
      info.put("size", file.length());
      info.put("modified", file.lastModified());
      
      // Copy it over
      if (file.length() == 0) {
        _log.info("skipping 0-length file: " + dfsPath);
      } else if (file.length() < EXPLODED_FILE_COPY_THRESHOLD) {
        _log.info("copying file to DFS: " + dfsPath);
        dfs.copyFile(file, dfsPath);
        info.put("dfs_uri", dfs.getUriFor(dfsPath));
      } else {
        _log.info("skilling file because it is too large: " + dfsPath);
      }
      fileList.add(info);
    }
    
    // Done
    return fileList;
    
  }

  
}
