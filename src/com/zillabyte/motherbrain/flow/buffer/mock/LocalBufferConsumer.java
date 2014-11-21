package com.zillabyte.motherbrain.flow.buffer.mock;

import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedList;

import net.sf.json.JSONObject;

import com.google.common.collect.Lists;
import com.zillabyte.motherbrain.api.APIException;
import com.zillabyte.motherbrain.api.RestAPIHelper;
import com.zillabyte.motherbrain.flow.MapTuple;
import com.zillabyte.motherbrain.flow.buffer.BufferConsumer;
import com.zillabyte.motherbrain.flow.buffer.SourceFromBuffer;
import com.zillabyte.motherbrain.flow.operations.OperationLogger;
import com.zillabyte.motherbrain.utils.Utils;


/**
 * A mock buffer for use in testing basic sources from buffers
 * @author sjarvie
 *
 */
public class LocalBufferConsumer implements BufferConsumer, Serializable {


  private static final long serialVersionUID = 4433057004414450020L;
  private SourceFromBuffer _source;
  private String _topic;
  private LinkedList<MapTuple> _messages = Lists.newLinkedList();


  private final int NUM_MESSAGES = 20;
  private int _messageNum = 0;
  
  

  public LocalBufferConsumer(SourceFromBuffer sourceOperation) {
    _source = sourceOperation;  
    _topic = _source.rawQuery();
    String authToken = sourceOperation.getTopFlow().getFlowConfig().getAuthToken();
    
    try {
      JSONObject job_id = RestAPIHelper.post("/relations/"+_topic+"/samples_anonymous", "", authToken);
      _source.logger().writeLog("Fetching relation data for "+_topic+". This may take a while...", OperationLogger.LogPriority.RUN);
      while(true) {
        JSONObject result = RestAPIHelper.post("/relations/"+_topic+"/poll_anonymous", job_id.toString(), authToken);

        if(result.getString("status").equalsIgnoreCase("completed")) {
          Iterator<?> aliasesIterator = result.getJSONObject("return").getJSONArray("column_aliases").iterator();
          JSONObject aliases = new JSONObject();
          while(aliasesIterator.hasNext()) {
            JSONObject alias = (JSONObject) aliasesIterator.next();
            aliases.put(alias.getString("alias"), alias.getString("concrete_name"));
          }
          Iterator<?> rowsIterator = result.getJSONObject("return").getJSONArray("rows").iterator();
          while(rowsIterator.hasNext()) {
            MapTuple m = MapTuple.create((JSONObject) rowsIterator.next());      
            m.addAliases(aliases);
            _messages.add(m);
          }
          break;
        }
        Utils.sleep(1000L);
      }
    } catch (APIException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public MapTuple getNextTuple() {
    
    if (_messageNum < NUM_MESSAGES){
      return _messages.get(_messageNum++);
    }
    return null;
  }

  @Override
  public boolean isEmitComplete() {
    return _messageNum == NUM_MESSAGES;
  }

  @Override
  public JSONObject createSnapshot() {
    JSONObject snapshot = new JSONObject();
    snapshot.put("messageNum", _messageNum);
    return snapshot;
  }

  @Override
  public void applySnapshot(JSONObject snapshot) {
    _messageNum = snapshot.getInt("messageNum");
  }

}
