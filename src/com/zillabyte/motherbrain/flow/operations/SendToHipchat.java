package com.zillabyte.motherbrain.flow.operations;

import net.sf.json.JSONObject;

import com.google.monitoring.runtime.instrumentation.common.com.google.common.base.Joiner;
import com.zillabyte.motherbrain.api.APIException;
import com.zillabyte.motherbrain.api.RestAPIHelper;

public class SendToHipchat {

	  public void sendToHipchat(String message, Exception e) {
		 
//		 if (rateLimiter.tryAcquire()) {

			 String stacktrace = new String();
			 stacktrace = Joiner.on("\n").join(e.getStackTrace());
			  
			 JSONObject params = new JSONObject();
			 params.put("message", message);
			 params.put("stack_trace", stacktrace);
		
			 // send to API
			 try {
			   RestAPIHelper.post("/gmb_errors_to_hipchat", params.toString(), "AjQl83zAuawmphDzoiH9Lps4RYRM4bl7RqjddZLnOoWWR-qauN5_RGdK");
			 } catch (APIException e1) {
			   e1.printStackTrace();
			 }
		 
//		 }

	  }
}
