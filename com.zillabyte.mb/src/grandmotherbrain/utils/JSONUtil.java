package grandmotherbrain.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import net.sf.json.JSON;
import net.sf.json.JSONArray;
import net.sf.json.JSONNull;
import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.log4j.Logger;

import com.google.common.collect.Maps;

public class JSONUtil {

  private static Logger _log = Utils.getLogger(JSONUtil.class);
  
  @SuppressWarnings("unchecked")
  public static void removeNulls(JSONObject json) {
    Iterator<String> keyIter = json.keys();
    while(keyIter.hasNext()) {
      String key = keyIter.next().toString();
      if (json.get(key) instanceof JSONNull || json.get(key) == null) {
        json.remove(key);
      }
    }
  }
  
  
  public static JSONObject parseObj(String o) {
    JSON j = JSONSerializer.toJSON(o);
    if (j instanceof JSONNull) {
      return null;
    } else {
      return (JSONObject)j;
    }
  }
  
  public static JSONArray parseArray(String o) {
    JSON j = JSONSerializer.toJSON(o);
    if (j instanceof JSONNull) {
      return null;
    } else {
      return (JSONArray)j;
    }
  }


  @SuppressWarnings({ "unchecked", "rawtypes" })
  private static Object getJsonType(Object input) {
    if (input instanceof Map) {
      
      Map ret = Maps.newHashMap();
      Map asMap = (Map) input;
      for(Object oo : asMap.entrySet()) {
        Entry e = (Entry) oo;
        if (e.getKey() instanceof String == false) {
          // illegal..
          return null;
        }
        ret.put(e.getKey(), getJsonType(e.getValue()));
      }
      return ret;

    } else if (input instanceof Collection) {
      
      Collection asList = (Collection) input;
      List ret = new ArrayList<>();
      Iterator i = asList.iterator();
      while(i.hasNext()) {
        ret.add(getJsonType(i.next()));
      }
      return ret;
      
    } else if (input instanceof Object[]) {

      Object[] asArray = (Object[]) input;
      Object[] ret = new Object[asArray.length];
      for(int i=0;i<asArray.length;i++) {
        ret[i] = getJsonType(asArray[i]);
      }
      return ret;
      
    } else if (input instanceof Integer) {
      return input;
    } else if (input instanceof Double) {
      return input;
    } else if (input instanceof Long) {
      return input;
    } else if (input instanceof String) {
      return input;
    } else if (input instanceof Float) {
      return input;
    } else if (input instanceof Enum) {
      return input;
    } else if (input instanceof Boolean){
      return input;
    } else if (input == null) {
      return input;
    }
    return null;
  }
  
  
  public static JSONObject toJSON(Map<String, Object> values) {
    return JSONObject.fromObject( getJsonType(values) );
  }
  
  
  public static JSONArray toJSON(Collection<Object> values) {
    return JSONArray.fromObject( getJsonType(values) );
  }
  
  public static String getStringForType(Object value) {
    if(value instanceof String) {
      return "\""+ StringEscapeUtils.escapeJson((String)value) +"\"";
    } else if(value instanceof Integer || value instanceof Float || value instanceof Long || value instanceof Boolean) {
      return value.toString();
    } else if(value instanceof JSONArray) {
      return JSONUtil.toString((JSONArray) value);
    } else if(value instanceof JSONObject) {
      return JSONUtil.toString((JSONObject) value);
    } else if(value instanceof JSONNull || value == null) {
      return "null";
    } else {
      throw new RuntimeException("unrecognized type for value: "+value+"["+value.getClass().getName()+"]");
    }
  }
  
  public static String toString(JSONObject o) {
    String outString = "{";
    for(Object key : o.keySet()) {
      outString += "\""+ StringEscapeUtils.escapeJson(key.toString())+"\":";
      Object value = o.get(key);
      outString += getStringForType(value)+","; 
    }
    if(outString.endsWith(",")) {
      outString = outString.substring(0, outString.length()-1)+"}"; //delete the last comma
    } else {
      outString += "}"; //if it's empty, then there's no trailing comma
    }
    return outString;
  }
  
  public static String toString(JSONArray o) {
    String outString = "[";
    Iterator<Object> iter = o.iterator();
    while(iter.hasNext()) {
      outString += getStringForType(iter.next())+",";
    }
    if(outString.endsWith(",")) {
      outString = outString.substring(0, outString.length()-1)+"]"; //delete the last comma
    } else {
      outString += "]";
    }
    return outString;
  }

}
