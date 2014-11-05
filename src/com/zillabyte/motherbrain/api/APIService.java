package com.zillabyte.motherbrain.api;

import java.util.Collection;

import com.zillabyte.motherbrain.flow.MapTuple;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;


public interface APIService {
 
  JSONObject getFlowSettings(String flowName, String authToken) throws APIException;

  JSONObject getRelationSettings(String relationName, String authToken) throws APIException;

  JSONObject getRelationConcretified(String flowId, String sql, String authToken)  throws APIException;

  JSONObject postRelationSettings(String appId, String relationName, JSONArray jsonSchema, String bufferType, String authToken) throws APIException;

  JSONObject postFlowState(String id, String newState, String authToken) throws APIException;

  JSONObject postFlowRegistration(String id, JSONObject schema, String authToken) throws APIException;

  JSONObject appendRelation(String relationId, Collection<MapTuple> buffer, String authToken)  throws APIException;
  
}
