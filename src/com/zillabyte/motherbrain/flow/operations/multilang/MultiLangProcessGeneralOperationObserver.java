package com.zillabyte.motherbrain.flow.operations.multilang;



import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.LinkedBlockingQueue;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.log4j.Logger;

import com.zillabyte.motherbrain.flow.MapTuple;
import com.zillabyte.motherbrain.flow.operations.Operation;
import com.zillabyte.motherbrain.flow.operations.OperationException;
import com.zillabyte.motherbrain.flow.operations.OperationLogger;
import com.zillabyte.motherbrain.utils.JSONUtil;


/***
 * Handles general work loads (i.e. responding to fails, logging, etc)
 * @author jake
 *
 */
public class MultiLangProcessGeneralOperationObserver implements MultiLangMessageHandler, MultiLangLogHandler, MultiLangErrorHandler {

  private static Logger _log = Logger.getLogger(MultiLangProcessGeneralOperationObserver.class);
  private MultiLangProcess _proc;
  private Operation _operation;
  private LinkedBlockingQueue<Exception> _errors = new LinkedBlockingQueue<>();
  
  
  public MultiLangProcessGeneralOperationObserver(MultiLangProcess proc, Operation operation) {
    _proc = proc;
    _proc.addMessageListener(this);
    _proc.addLogListener(this);
    _proc.addErrorListener(this);
    _operation = operation;
  }


  @Override
  public void onStdErr(String s, Logger fallbackLogger) {
    _operation.logger().writeLog(s, OperationLogger.LogPriority.ERROR);
  }


  @Override
  public void onStdOut(String s, Logger fallbackLogger) {
    // Send stdout messages that are not part of our protocol.  Kinda hacky
    if (s.equalsIgnoreCase("end")) return;
    if (s.startsWith("{")) return;
    if (s.length() == 0) return;
    _operation.logger().writeLog(s, OperationLogger.LogPriority.RUN);
  }


  @Override
  public void onSystemError(String s, Logger fallbackLogger) {
    _operation.logger().writeLog(s, OperationLogger.LogPriority.ERROR);
  }


  @Override
  public void onSystemInfo(String s, Logger fallbackLogger) {
    _operation.logger().writeLog(s, OperationLogger.LogPriority.SYSTEM);
  }

  
  
  /***
   * 
   */
  public void detach() {
    this._proc.removeLogListener(this);
    this._proc.removeMessageListener(this);
    this._proc.removeErrorListener(this);
  }
  
  
  /***
   * 
   * @param obj
   */
  protected void handleJsonMessage(JSONObject obj) {
    // For subclasses
  }

  
  /**
   * @throws InterruptedException
   * 
   */
  @Override
  public void handleMessage(String line) throws OperationException, InterruptedException {

    // Sanity
    if (line == null) return;
    if (line.equals("end")) return;
    if (line.length() == 0) return;
    
    // Init
    JSONObject obj = JSONUtil.parseObj(line);
    
    
    if (obj.has("ping")) {
      
      // The process is making sure we're alive.  Respond with a 'pong'
      try {
        _proc.writeMessageWithEnd("{\"pong\": \"" + System.currentTimeMillis() + "\"}");
      } catch (MultiLangProcessException e) {
        throw new OperationException(_operation, e);
      }
    
    } else if (obj.has("command") && obj.getString("command").equalsIgnoreCase("fail")) {
      
      // Errors! 
      _log.error("error: " + obj.getString("msg"));
      _operation.logger().writeLog(obj.getString("msg"), OperationLogger.LogPriority.ERROR);
      
      // Tell the operation to ERROR
      throw (OperationException)
        new OperationException(_operation)
        .setUserMessage(obj.getString("msg"))
        .setInternalMessage(obj.getString("msg"));
      
    } else if (obj.has("command") && obj.getString("command").equalsIgnoreCase("log")) {
      _operation.logger().writeLog(obj.getString("msg"), OperationLogger.LogPriority.RUN);
      
    } else {
      handleJsonMessage(obj);
    }
    
  }


  
  /***
   * A helper method to send message down
   * @param t
   * @throws MultiLangProcessDeadException 
   * @throws InterruptedException 
   */
  public void sendTupleMessage(MapTuple t) throws MultiLangProcessException, InterruptedException {

    JSONObject meta = new JSONObject();
    meta.put("confidence", t.meta().getConfidence());
    meta.put("source", t.meta().getSource());
    meta.put("date", Long.valueOf(t.meta().getSince().getTime()));
    
    // CLI: Make sure this is synced
    JSONObject obj = new JSONObject();
    obj.put("meta", meta);
    obj.put("tuple", t.getValuesJSON());
    
    Map<String, String> aliases = t.getAliases();
    if (aliases.size() > 0) {
      JSONArray ary = new JSONArray();
      for(Entry<String, String> e: aliases.entrySet()) {
        JSONObject o = new JSONObject();
        o.put("concrete_name", e.getValue());
        o.put("alias", e.getKey());
        ary.add(o);
      }
      obj.put("column_aliases", ary);
    }

    // _log.info("sending tuple message: " + obj.toString());
    _proc.writeMessageWithEnd(obj.toString());

  }
  
  
  
  
  
  
  
  /***
   * 
   * @param t
   * @throws MultiLangProcessDeadException 
   * @throws InterruptedException 
   */
  public void sendBeginGroup(MapTuple t) throws MultiLangProcessException, InterruptedException {
    
    JSONObject meta = new JSONObject();
    meta.put("confidence", t.meta().getConfidence());
    meta.put("source", t.meta().getSource());
    meta.put("date", Long.valueOf(t.meta().getSince().getTime()));
    
    // CLI
    JSONObject obj = new JSONObject();
    obj.put("command", "begin_group");
    obj.put("meta", meta);
    obj.put("tuple", t.getValuesJSON());
    
    // _log.info("sending group begin message: " + obj.toString());
    _proc.writeMessage(obj.toString());
    _proc.writeMessage("end");
    
  }
  
  
  
  /***
   * 
   * @param t
   * @param aliases
   * @throws MultiLangProcessDeadException 
   * @throws InterruptedException 
   */
  public void sendAggregate(MapTuple t, JSONArray aliases) throws MultiLangProcessException, InterruptedException {
    
    JSONObject meta = new JSONObject();
    meta.put("confidence", t.meta().getConfidence());
    meta.put("source", t.meta().getSource());
    meta.put("date", Long.valueOf(t.meta().getSince().getTime()));
    
    // CLI
    JSONObject obj = new JSONObject();
    obj.put("command", "aggregate");
    obj.put("meta", meta);
    obj.put("tuple", t.getValuesJSON());
    
    if (aliases != null) {
      obj.put("column_aliases", aliases);
    }
  
    //_log.debug("sending aggregate message: " + obj.toString());
    _proc.writeMessage(obj.toString());
    _proc.writeMessage("end");
    
  }
  
  
  
  /**
   * @throws MultiLangProcessDeadException 
   * @throws InterruptedException *
   * 
   */
  public void sendEndGroup() throws MultiLangProcessException, InterruptedException {
    
    JSONObject obj = new JSONObject();
    obj.put("command", "end_group");
    
    //_log.info("sending end group message: " + obj.toString());
    _proc.writeMessage(obj.toString());
    _proc.writeMessage("end");
  
  }




  @Override
  public void handleError(Exception ex) {
    _log.info("error captured: " + ex);
    _errors.add(ex);
  }


  @Override
  public Exception getNextError() {
    return _errors.poll();
  }

  @Override
  public void mabyeThrowNextError() throws OperationException {
    final Exception ex = getNextError();
    if (ex != null) {
      throw new OperationException(this._operation, ex);
    }
  }
}

