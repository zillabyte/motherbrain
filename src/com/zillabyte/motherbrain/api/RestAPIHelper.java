package com.zillabyte.motherbrain.api;

import java.util.concurrent.TimeUnit;

import net.sf.json.JSONException;
import net.sf.json.JSONObject;

import org.apache.log4j.Logger;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.WebResource.Builder;
import com.zillabyte.motherbrain.universe.Config;
import com.zillabyte.motherbrain.utils.JSONUtil;
import com.zillabyte.motherbrain.utils.Utils;

public class RestAPIHelper {
  
  protected static Logger log = Logger.getLogger(RestAPIHelper.class);
  public static final int MAX_RETRIES = 5;
  public static final long RETRY_SLEEP_MS = TimeUnit.MILLISECONDS.convert(3, TimeUnit.SECONDS);

  public RestAPIHelper(APIService api) {
  }
  
  public static String getHost(){
    return Config.getOrDefault("api.host", "localhost");
  }
  
  public static int getPort() {
    return Config.getOrDefault("api.port", Utils.valueOf(5000)).intValue();
  }

  public static JSONObject get(String path, String authToken) throws APIException {
    Client client = Client.create();
    String url = "http://" + getHost() + ":" + getPort() + path;
    ClientResponse response;
    int retries = 0;
    
    log.info("get: " + url);
    
    do {
      retries++;
      WebResource webResource = client.resource(url);
      
      Builder b = webResource
          .accept("application/json");
      if (authToken != null && !authToken.isEmpty())
          b.header("Authorization", "auth_token " + authToken);
      response = b.get(ClientResponse.class);
      
      if (response.getStatus() == 200) {
        break;
      } else if (response.getStatus() >= 500) {
        log.info("Server responded with 500. Retrying in a few seconds...(" + retries + ")");
        Utils.sleep(RETRY_SLEEP_MS);
      } else {
         throw new APIException("Failed : HTTP error code : " + response.getStatus() + " " + response.getEntity(String.class));
      }
    } while(retries < MAX_RETRIES);
 
    final String output = response.getEntity(String.class);
    if (retries == MAX_RETRIES) {
      throw new APIException("Failed: max retries exceeded: response from API: " + output);
    }

    log.debug("get returned: " + output);

    JSONObject ret = JSONUtil.parseObj(output);

    if (ret == null) {
      throw new APIException("Failed: returned response body was null for output: "+output);
    }

    log.info("done: " + url);
    return ret;
  }
  
  
  public static JSONObject post(String path, String body, String authToken) throws APIException {
    Client client = Client.create();
    String url = "http://" + getHost() + ":" + getPort() + path;

    log.info("post: " + url + " body: " + body);

    WebResource webResource = client.resource(url);
    ClientResponse response;
    
    int retries = -1;
    do {
      retries++; 
      Builder b = webResource
          .type("application/json")
          .accept("application/json");
      if (authToken != null && !authToken.isEmpty()) 
          b.header("Authorization", "auth_token " + authToken);
      response = b.post(ClientResponse.class, body);
   
      if (response.getStatus() == 200) {
        break;
      } else if (response.getStatus() >= 500) {
        
        // See if this is a formatted error from the server... 
        String output = response.getEntity(String.class);
        log.info("Server responded with 500. Retrying in a few seconds...(" + retries + ") (" + path + "): " + Utils.truncate(output));
        JSONObject ret; 
        try { 
          
          // It's a formatted message.. pass it on to the user... 
          ret = JSONUtil.parseObj(output);
          if (ret.containsKey("error_message")) {
            throw new APIException(ret.getString("error_message"));
          }
          
        } catch(JSONException ex) {
          Utils.sleep(RETRY_SLEEP_MS);
        }
      } else {
        throw new APIException("Failed : HTTP error code : " + response.getStatus());
      }
    } while (retries < MAX_RETRIES);

    if (retries == MAX_RETRIES) {
      throw new APIException("Failed: max retries exceeded.");
    }

    String output = response.getEntity(String.class);
    log.info("post returned: " + output);
    JSONObject ret = JSONUtil.parseObj(output);
    if (ret == null) // Should not be NULL, it's just a string conversion.
      throw new APIException("Failed: returned response body was null for output: "+output);
    return ret;
    
  }
  
  public static JSONObject put(String path, String body, String authToken) throws APIException {
    Client client = Client.create();
    String url = "http://" + getHost() + ":" + getPort() + path;

    log.info("put: " + url + " body: " + body);

    WebResource webResource = client.resource(url);

    ClientResponse response;

    int retries = 0;
    do {
      
      Builder b = webResource
          .type("application/json")
          .accept("application/json");
      if (authToken != null && !authToken.isEmpty())
          b.header("Authorization", "auth_token " + authToken);
      response = b.put(ClientResponse.class, body);
   
      if (response.getStatus() == 200) {
        break;
      } else if (response.getStatus() >= 500) {
        log.info("Server responded with 500. Retrying in a few seconds...(" + retries + ")");
        try {
          Thread.sleep(RETRY_SLEEP_MS);
        } catch (InterruptedException e) {
          throw new APIException(e);
        }
      } else {
        throw new APIException("Failed : HTTP error code : " + response.getStatus());
      }
      retries++;

    } while (retries < MAX_RETRIES);

    if (retries == MAX_RETRIES) {
      throw new APIException("Failed: max retries exceeded.");
    }

    String output = response.getEntity(String.class);
    log.debug("post returned: " + output);
    
    JSONObject ret = JSONUtil.parseObj(output);
    if (ret == null) // Should not be NULL
      throw new APIException("Failed: returned response body was null for output: "+output);
    return ret;
    
  }
}
