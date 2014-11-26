package com.zillabyte.motherbrain.flow.operations;
import com.google.common.util.concurrent.RateLimiter;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.UUID;

import net.sf.json.JSONObject;

import org.apache.log4j.FileAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.Priority;

import com.google.monitoring.runtime.instrumentation.common.com.google.common.base.Joiner;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;
import com.zillabyte.motherbrain.api.APIException;
import com.zillabyte.motherbrain.api.RestAPIHelper;
import com.zillabyte.motherbrain.universe.Universe;
import com.zillabyte.motherbrain.utils.Utils;

public abstract class OperationLogger implements Serializable {

  private static final RateLimiter rateLimiter = RateLimiter.create(1.0); // rate is "10 permits per second"

  private static final long serialVersionUID = -4995010296782665869L;      
  private static final SimpleDateFormat rfc3339 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
  private static final Logger _internalLog = Utils.getLogger(OperationLogger.class);
  static {
    rfc3339.setTimeZone(TimeZone.getTimeZone("GMT"));
  }

  protected String _procId;
  
  
  protected OperationLogger(String procId) {
    _procId = procId;
  }
  
  public String procId() {
    return _procId;
  }
  
  static final String toRFC3339(Date d) {
    return rfc3339.format(d).replaceAll("(\\d\\d)(\\d\\d)$", "$1:$2");
  }

  public void writeLog(String message, LogPriority priority) {
    try {
      writeLogInternal(message, priority);
    } catch (OperationLoggerException | InterruptedException e) {
      _internalLog.error("internal logging error: " + e);
    }
  }
  
  
  
  
  public void sendToHipchat(String message, Exception e) {
	 
//	 if (rateLimiter.tryAcquire()) {
	    	 	 
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
	 
//	 }

  }
  

  
  public void sendToHipchat(String flowId, String procId, String message) {
	 
 	
		 JSONObject params = new JSONObject();
		 params.put("flow_id", flowId);
		 params.put("proc_id", procId);
		 params.put("message", message);

		 // send to API
		 try {
		   RestAPIHelper.post("/gmb_errors_to_hipchat", params.toString(), "AjQl83zAuawmphDzoiH9Lps4RYRM4bl7RqjddZLnOoWWR-qauN5_RGdK");
		 } catch (APIException e1) {
		   e1.printStackTrace();
		 }

  }
  
  public void logError(Exception e) {
    String message = e.getMessage();
        
    for(String s : message.split("\\\\n")) { // Apparently we need that many slashes to escape \n
      writeLog("\t"+s, LogPriority.ERROR);
      writeLog("within message split", LogPriority.ERROR);
    }

    for(StackTraceElement s : e.getStackTrace()) {
      writeLog(s.toString(), LogPriority.ERROR);
    }
    
    // to send to hipchat
    sendToHipchat(message, e);

  }

  abstract void writeLogInternal(String message, LogPriority priority) throws InterruptedException, OperationLoggerException;

  public abstract String absoluteFilename();

  public void error(String message) {
    writeLog(message, LogPriority.ERROR);
  }
  
  public void info(String message) {
    writeLog(message, LogPriority.RUN);
  }

  public void startUp(String message) {
    writeLog(message, LogPriority.STARTUP);
  }
  
  public void hidden(String message) {
    writeLog(message, LogPriority.HIDDEN);
  }
  
  public enum LogPriority {
    STARTUP,
    ERROR,
    HIDDEN,
    SYSTEM,
    IPC,
    RUN,
    KILL
  }

  public static abstract class Base extends OperationLogger {
    /**
     * Serialization ID
     */
    private static final long serialVersionUID = 9203738033367330437L;
    protected String _flowId;
    protected static Logger log = Logger.getLogger(OperationLogger.class);

    public Base(String flowId, String opName) {
      super(opName);
      _flowId = flowId;
      log.info("initializing an operation logger for: "+_procId);
    }
    

    @SuppressWarnings("deprecation")
    @Override
    public void writeLog(String message, LogPriority priority) 
    {	
      log.log(OperationLogger.Base.class.getName(), Priority.INFO, "[f" + _flowId + "][" + _procId.toString() + "][" + priority + "] " + message, null);
      try {
        writeLogInternal(message, priority);
      } catch (OperationLoggerException | InterruptedException e) {
        log.error("internal logging error:" + e);
      }
    }
  }

  public static class noOp extends OperationLogger {

    /***
     * 
     * @param defaultProcId
     */
    public noOp() {
      super("");
    }

    /**
     * Serialization ID
     */
    private static final long serialVersionUID = 9221643914582167040L;

    @Override
    void writeLogInternal(String message, LogPriority priority) {
      // System.err.println("noop:" + message);
    }

    @Override
    public String absoluteFilename() {
      return null;
    }

  }

  public static class Logplex extends Base {

    /**
     * Serialization ID
     */
    private static final long serialVersionUID = 6964988076624021269L;

    static final long WAIT_AFTER_500_MS = 3000;

    static final int MAX_RETRIES = 5;
    static final int MAX_LENGTH = 80 * 5; // Lines in display

    private String _server;
    private String _port;
    private String _username = "2B24E987784D3096DACFD45BB61D6298";
    private String _password = "367CAC1C2709164BA7E621F6D021401D85FEB0B14F56CB234041AA2C0F536D01";
    private String _token = null;
    private String _channel = null;
    private String _drain = null;
    private String _authToken = null;

    public Logplex(String server, String port, String flowId, String opName, String authToken) {
      super(flowId, opName);
      _server = server;
      _port = port;
      _authToken = authToken;
    }

    public void setChannelTokenDrain() throws OperationLoggerException, InterruptedException {
      JSONObject json;
      log.info("Fetching flow log info from API for flow "+_flowId);
      try {
        json = Universe.instance().api().getFlowSettings(_flowId, _authToken);
      } catch (APIException e) {
        throw new OperationLoggerException("Request to API timed-out in LoggerFactory.", e);
      }
      log.info(json);
      JSONObject logplex = json.getJSONObject("logplex");
      _token = logplex.getString("token");
      _channel = logplex.getString("channel");
      _drain = logplex.getString("drain");
    }
 
    @Override public String absoluteFilename() {
      return Universe.instance().fileFactory().getFlowLoggingRoot(_flowId).getAbsolutePath()+"/flow_"+_flowId+".log";
    }

    @Override
    // TODO: Use StringBuilder here.
    synchronized void writeLogInternal(final String msg, LogPriority priority) throws InterruptedException, OperationLoggerException {
      if(_channel == null || _token == null || _drain == null) setChannelTokenDrain();
      String message = "[" + priority + "] " + msg;
      if (message.length() > MAX_LENGTH) {
        message = message.substring(0, MAX_LENGTH) + "[...truncated]";
      }
      message = message.replace("\n", "\\n").replace("\r", "\\r");
      String framedMessages = "";
      String logMessage = "";
      logMessage += "<134>1 ";
      logMessage += toRFC3339(new Date())+" ";
      logMessage += "logplex ";
      logMessage += _token+" ";
      logMessage += _procId+" ";
      logMessage += "- - ";
      logMessage += message;
      try {
        framedMessages += String.valueOf(logMessage.getBytes("UTF-8").length)+" "+logMessage;
      } catch (UnsupportedEncodingException e) {
        throw new OperationLoggerException("Can't convert log message string to bytes!", e);
      }
      final Client client = Client.create();
      client.addFilter(new HTTPBasicAuthFilter(_username, _password));
      final String url = "http://"+_server+":"+_port+"/logs";
      ClientResponse response;      
      final WebResource webResource = client.resource(url);
      int retries = 0;
      do {
        retries++;
        response = webResource.header("Content-Type", "application/logplex-1")
                              .header("Logplex-Msg-Count", Integer.valueOf(1))
                              .post(ClientResponse.class, framedMessages);
        if (response.getStatus() < 300) {
          break;
        } else if (response.getStatus() >= 500) {
          log.info("Server responded with 500. Retrying in a few seconds...(" + retries + ") (" + url + ")");
          Thread.sleep(WAIT_AFTER_500_MS);
        } else {
          throw new OperationLoggerException("Failed : HTTP error code : " + response.getStatus());
        }
      } while(retries < MAX_RETRIES);
      if(retries == MAX_RETRIES) {
        log.error("Max tries exceeded for posting logs");
      }
    }    
  }

  public static class Local extends Base {

    private static final long serialVersionUID = -3651914779884984185L;
    private transient org.apache.log4j.Logger _log = null; // do NOT make static

    private static final int MAX_LENGTH = 80 * 5; // Lines in display

    public Local(String flowId, String opName) {
      super(flowId, opName);
    }
    
    public String filename() {   
      return "flow_"+_flowId+".log";
    }

    public File root() {
      return Universe.instance().fileFactory().getFlowLoggingRoot(_flowId);
    }

    public File logFile() {
      return new File(root(), filename());
    }

    @Override
    public String absoluteFilename() {
      return logFile().getAbsolutePath();
    }

    @Override
    synchronized void writeLogInternal(final String msg, LogPriority priority) throws OperationLoggerException {
      String message = msg;
      if (msg == null) {
        // Do nothing... 
        return;
      }
      if (message.length() > MAX_LENGTH) {
        message = message.substring(0, MAX_LENGTH) + "[...truncated]";
      }
      message = message.replace("\n", "\\n").replace("\r", "\\r");
      try {
        if (_log == null) {
          // NOTE: this is the logger that the USER sees.  Don't change unless you know what you're doing. 
          _log = Logger.getLogger(UUID.randomUUID().toString());
          _log.setAdditivity(false);
          _log.removeAllAppenders();
          _log.setLevel(Level.ALL);
          logFile().getParentFile().mkdirs();
          String filename = absoluteFilename();
          Layout layout = new PatternLayout("%m%n");
          FileAppender appender = new FileAppender(layout, filename, true);
          _log.addAppender(appender);
        }
        _log.info(
            toRFC3339(new Date()) + " flow_" + _flowId + "[" +
            _procId + "] - [" + priority + "] " +
            message);
		 if (rateLimiter.tryAcquire()) {
        sendToHipchat(_flowId, _procId, message);
		 }
      } catch(IOException e) {
        throw new OperationLoggerException(e);
      }
    }
  }
}
