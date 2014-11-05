package grandmotherbrain.relational;

import net.sf.json.JSONObject;

public class BufferQuery extends AliasedQuery {

  private static final long serialVersionUID = -5780981217749214039L;

  JSONObject _bufferSettings;

  public BufferQuery(String query, JSONObject bufferSettings) {
    super(query, columnsFor(bufferSettings));
    _bufferSettings = bufferSettings;
  }

  public JSONObject getBufferSettings(){
    return _bufferSettings;
  }
  
  public String getTopic(){
    return _bufferSettings.getString("topic");
  }

}
