package com.zillabyte.motherbrain.flow.operations.multilang.operations;


import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import net.sf.json.JSONObject;

import com.google.monitoring.runtime.instrumentation.common.com.google.common.collect.Lists;
import com.zillabyte.motherbrain.api.APIException;
import com.zillabyte.motherbrain.api.RestAPIHelper;
import com.zillabyte.motherbrain.flow.Fields;
import com.zillabyte.motherbrain.flow.MapTuple;
import com.zillabyte.motherbrain.flow.collectors.OutputCollector;
import com.zillabyte.motherbrain.flow.graph.Connection;
import com.zillabyte.motherbrain.flow.operations.Function;
import com.zillabyte.motherbrain.flow.operations.OperationException;
import com.zillabyte.motherbrain.flow.operations.multilang.MultiLangException;
import com.zillabyte.motherbrain.utils.Utils;


public final class LocalComponent extends Function {

  private static final long serialVersionUID = -6784282062699640773L;
  
  private String _componentName;
  private JSONObject _componentMeta;
  private Map<String, String> _inputFields = new LinkedHashMap<String, String>();
  private Map<String, String> _outputFields = new LinkedHashMap<String, String>();
  
  public LocalComponent(JSONObject nodeSettings) {
    super(MultilangHandler.getName(nodeSettings), MultilangHandler.getConfig(nodeSettings));
    _componentName = nodeSettings.getString("id");
    try {
      _componentMeta = RestAPIHelper.get("/flows/"+_componentName+"/show_anonymous", "");
    } catch (APIException e) {
      throw new RuntimeException(e);
    }
    
    // Construct the input fields and output fields
    Iterator<?> nodeIterator = _componentMeta.getJSONArray("nodes").iterator();
    while(nodeIterator.hasNext()) {
      JSONObject node = (JSONObject) nodeIterator.next();
      if(node.getString("type").equalsIgnoreCase("input")) {
        Iterator<?> iFieldsIterator = node.getJSONArray("fields").iterator();
        while(iFieldsIterator.hasNext()) {
          JSONObject fields = (JSONObject) iFieldsIterator.next();
          String iField = (String) fields.keys().next();
          String iType = fields.getString(iField);
          _inputFields.put(iField, iType);
        }
      } else if(node.getString("type").equalsIgnoreCase("output")) {
        Iterator<?> iFieldsIterator = node.getJSONArray("columns").iterator();
        while(iFieldsIterator.hasNext()) {
          JSONObject fields = (JSONObject) iFieldsIterator.next();
          String iField = (String) fields.keys().next();
          String iType = fields.getString(iField);
          _outputFields.put(iField, iType);
        }
      }
    }
  }

  @Override
  public void prepare() throws MultiLangException {
  }

  
  @Override
  public void cleanup() throws MultiLangException, InterruptedException {
  }


  @Override
  public void process(final MapTuple t, final OutputCollector collector) throws OperationException, InterruptedException {
    try {
      // Translate the map tuple into a list of just the values (order is important, so we need to loop through the fields one by one).
      // This will be passed to a running rpc as an argument
      List<Object> tupleValues = Lists.newArrayList();
      for(String field : _inputFields.keySet()) {
        tupleValues.add(t.get(field));
      }
      List<Collection<Object>> tupleList = new ArrayList<Collection<Object>>();
      tupleList.add(tupleValues);
      
      // Send the rpc request
      JSONObject body = new JSONObject();
      body.put("rpc_inputs", tupleList);
      JSONObject reply = RestAPIHelper.post("/components/"+_componentName+"/execute_anonymous", body.toString(), "");
      Collection<?> execId = reply.getJSONObject("execute_ids").values();
      JSONObject execBody = new JSONObject();
      execBody.put("execute_ids", execId);
      
      // Continue to ping the rpc until it is done
      while(true) {
        JSONObject rpcStatus = RestAPIHelper.post("/components/"+_componentName+"/results_anonymous", execBody.toString(), "");
        if(rpcStatus.containsKey("results")) {
          JSONObject rpcResults = (JSONObject) rpcStatus.getJSONObject("results").values().iterator().next();
          if(rpcResults.containsKey("data")) {
            Iterator<?> rpcTupleIterator = rpcResults.getJSONObject("data").getJSONArray("rpc_output").iterator();
            while(rpcTupleIterator.hasNext()) {
              JSONObject emitTuple = (JSONObject) rpcTupleIterator.next();
              collector.emit(MapTuple.create(emitTuple));
            }
            break;
          }
        }
        Utils.sleep(1000L);
      }
    
    } catch (APIException e) {
      throw (OperationException) new OperationException(this,e).setAllMessages("Error processing tuple: "+t.toString()+" (via remote RPC for component \""+_componentName+"\").");
    }
  }

  
  
  @Override 
  public boolean isAlive() {
    return true;
  }
  
  
  @Override
  public void onFinalizeDeclare() throws OperationException, InterruptedException {
    super.onFinalizeDeclare();
  }

  @Override
  public void onSetExpectedFields() throws OperationException {
    for(final Connection c : this.prevConnections()) {

      for(String field : _inputFields.keySet()) {
        c.source().addExpectedFields(c.streamName(), new Fields(field));
      }

    }
    super.onSetExpectedFields();
  }
  
}
