package com.zillabyte.motherbrain.flow.rpc;

import com.zillabyte.motherbrain.flow.App;
import com.zillabyte.motherbrain.flow.Component;
import com.zillabyte.motherbrain.flow.Flow;

public class RPCHelper {

  public static App wrapComponentInRpcApp(Component comp) {
    
    // Init 
    App app = new App(comp.getId(), comp.getName() + "_rpc");
    app.setMeta(comp.getMeta());
    app.getFlowConfig().setAll(comp.getFlowConfig());
    app.getFlowConfig().set("rpc", true);
    
    // Create an RPC source, tie it into the graph..
    comp.setParentFlow(app);
    app.graph().inject("rpc", comp.graph());
    RPCSource source = new RPCSource("rpc_input");
    source.setContainerFlow(app);
    app.graph().connect(source, comp.getOneInput(), "rpc_input_stream");
    
    RPCSink sink = new RPCSink("rpc_output");
    sink.setContainerFlow(app);
    app.graph().connect(comp.getOneOutput(), sink, "rpc_output_stream");
    
    return app;
  }
  
  
  public static boolean isRpcApp(Flow flow) {
    if (flow.getName().endsWith("_rpc")) { 
      return true;
    } else {
      return false;
    }
  }

}

