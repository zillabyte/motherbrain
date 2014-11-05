package com.zillabyte.motherbrain.api;


import java.io.Serializable;

import com.zillabyte.motherbrain.flow.config.FlowConfig;
import com.zillabyte.motherbrain.relational.BufferQuery;
import com.zillabyte.motherbrain.relational.ColumnDef;
import com.zillabyte.motherbrain.relational.Query;
import com.zillabyte.motherbrain.universe.Universe;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class RelationsHelper implements Serializable {

  /**
   * 
   */
  private static final long serialVersionUID = -990115807507923487L;
  private APIService _base;

  static final int initialURIBufferCapacity = 128;

  public RelationsHelper(APIService api) {
    this._base = api;
  }

  public JSONObject getRelationConfig(FlowConfig config, String relationName) throws APIException, InterruptedException {
    final JSONObject obj = _base.getRelationSettings(relationName, config.getAuthToken());
    return obj;
  }

  public Query concretifyQuery(String flowId, String relationName, String authToken) throws APIException, InterruptedException {
    JSONObject obj = _base.getRelationConcretified(flowId, relationName, authToken);
    
    Query result = null;
    if (obj.has("s3_only")) {
      // TODO: Migration for existing relation, not a priority.
    } else if (obj.has("buffer_settings")){
      final JSONObject bufferSettings = obj.getJSONObject("buffer_settings");
      
      if (bufferSettings == null){
        throw new NoSuchFieldError("missing buffer_settings");
      }
      result = new BufferQuery(relationName, bufferSettings);
    }
    return result;
  }


  public JSONObject postRelationConfig(FlowConfig config, String relationName, ColumnDef... columns) throws APIException, InterruptedException {
    JSONArray jsonSchema = new JSONArray();
    for (final ColumnDef column : columns) {
      final JSONObject jsonColumn = new JSONObject();
      jsonColumn.put(column.getAliases().get(0), column.getDataType().toString().toLowerCase());
      jsonSchema.add(jsonColumn);
    }      
    JSONObject obj = _base.postRelationSettings(config.getFlowId(), relationName, jsonSchema, (String)config.get("buffer_type", "s3"), config.getAuthToken());
    return obj;
  }
  
  public static RelationsHelper instance() {
    return new RelationsHelper(Universe.instance().api()); 
  }
}
