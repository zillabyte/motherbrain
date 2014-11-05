package com.zillabyte.motherbrain.flow;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import net.sf.json.JSONNull;
import net.sf.json.JSONObject;

import org.apache.commons.lang.StringUtils;
import org.eclipse.jdt.annotation.NonNullByDefault;
import org.eclipse.jdt.annotation.Nullable;

import com.google.common.collect.Lists;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.zillabyte.motherbrain.utils.JSONUtil;
import com.zillabyte.motherbrain.utils.queue.ByteSizable;

@NonNullByDefault
public class MapTuple implements Serializable, ByteSizable {
  
  private static final long serialVersionUID = 4458658589928148304L;
  public static final List<String> RESERVED = Lists.newArrayList("source", "confidence", "since");
  public static final MapTuple EMPTY = new MapTuple();
  
  final HashMap<String, Object> _values;
  final MetaTuple _meta;
  final HashMap<String, String> _aliases;
  
  
  public MapTuple(final MetaTuple m) {
    _meta = m;
    _values = new HashMap<>();
    _aliases = new HashMap<>(); 
  }
  
  public MapTuple(final MapTuple m) {
    _meta = m.meta();
    _values = new HashMap<>(m._values);
    _aliases = new HashMap<>(m._aliases);
  }
  
  
  public MapTuple() {
    this(new MetaTuple());
  }
  
  public final MetaTuple meta() {
    return _meta;
  }
  
  
  public final Map<String, Object> values() {
    return _values;
  }
  
  public final @Nullable Object get(final String field) {
    final Object value = _values.get(field);
    if (value != null) {
      return value;
    }
    // Meta field? 
    if (MetaTuple.confidenceColumn().getName().equals(field)) {
      return _meta.getConfidence();
    } else if(MetaTuple.sinceColumn().getName().equals(field)) {
      return _meta.getSince();
    } else if(MetaTuple.sourceColumn().getName().equals(field)) {
      return _meta.getSource();
    }
    return null;
  }

  public final Object getOrException(final String field) {
    final Object value = _values.get(field);
    if (value == null) {
      throw new NullPointerException("Field " + field + " not found!");
    }
    return value;
  }

  public final MapTuple put(final String field, final @Nullable Object obj) {
    if (obj == null) {
      /*
       * We need this because of joins.  We don't use Java null because
       * too many libraries either return null to represent absence,
       * don't allow null entries, or use null to represent something else.
       */
      _values.put(field, JSONNull.getInstance());
    } else {
      _values.put(field, obj);
    }
    return this;  // for chaining
  }
  
  public final MapTuple remove(String field) {
    _values.remove(field);
    return this;  // for chaining
  }

  
  // alias
  public final MapTuple add(String field, Object obj) {
    return put(field, obj);
  }
  
  public final MapTuple setMeta(int index, Serializable obj) {
    _meta.set(index, obj);
    return this;  // for chaining
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{");
    sb.append("{");
    for(Entry<String, Object> e : this._values.entrySet()) {
      sb.append("\"");
      sb.append(e.getKey());
      sb.append("\"=");
      if (e.getValue() instanceof String) {
        sb.append("\"");
      }
      if (e.getValue() == null) {
        sb.append("null");
      } else { 
        sb.append(StringUtils.abbreviate(e.getValue().toString().replace("\n", "\\n").replace("\r", "\\r"), 40));
      }
      if (e.getValue() instanceof String) {
        sb.append("\"");
      }
      sb.append(",");
    }
    sb.append("}");
    sb.append(",");
    sb.append( _meta.toString() );
    sb.append("}");
    final String string = sb.toString();
    assert (string != null);
    return string;
  }

  public final int sizeFull() {
    return this.sizeValues() + sizeMeta();
  }
  
  public final int sizeValues() {
    return this.values().size();
  }
  
  public final int sizeMeta() {
    this.meta();
    return MetaTuple.size();
  }

  public final long getApproxMemSize() {
    // Hacky: we assume strings are the main consumer of memory. 
    // everything else we assume 4 bytes. 
    long l = 0;
    for(Object o : this.values().values()) {
      if (o instanceof String) {
        l += ((String)o).length();
      } else {
        l += 4;
      }
    }
    l += (10);
    return l;
  }


  
  /*** 
   * Used mostly for testing.
   * @param val
   * @return T if all the values in val are found in this.  Note that 'this' may have fields not in 'val'
   */
  public final boolean containsAllValues(Map<String, ?> val) {
    for(Entry<String, ?> e : val.entrySet()) {
      if (this._values.containsKey(e.getKey()) && this._values.get(e.getKey()).equals(e.getValue()) ) {
        // Found! 
      } else {
        return false;
      }
    }
    return true;
  }

  
  
  public final boolean equalsTuple(Object oo, boolean print) {
    if (oo != null && oo instanceof MapTuple) {
      MapTuple t = (MapTuple)oo;
      MapDifference<String, Object> diff = Maps.difference(t._values, this._values);
      if (diff.areEqual()) {
        return true;
      } else {
        if (print) {
          System.err.println(diff);
        }
      }
    }
    return false;
  }
  
  public final boolean equalsTuple(Object oo) {
    return equalsTuple(oo, false);
  }
  
  public final boolean equalsTupleP(Object oo) {
    return equalsTuple(oo, true);
  }
  
  
  /***
   * 
   */
  public boolean equals(Object oo) {
    return equalsTuple(oo);
  }
  

  public final boolean containsValueKey(String col) {
    return this._values.containsKey(col);
  }
  
  public final Set<String> getValueKeys() {
    final Set<String> keySet = this._values.keySet();
    assert (keySet != null);
    return keySet;
  }


  public final JSONObject getValuesJSON() {
    return JSONUtil.toJSON(this._values);
  }

  public final void setValuesJSON(JSONObject values) {
    for (Object key : values.keySet()){
      String s = (String) key;
      _values.put(s, values.getString(s));
    }    
  }

  public final void addAlias(String alias, String concrete) {
    _aliases.put(alias, concrete);
  }
  
  public final void addAliases(JSONObject aliases) {
    for(Object alias : aliases.keySet()) {
      _aliases.put((String) alias, (String) aliases.get(alias));
    }
  }


  public final HashMap<String, String> getAliases() {
    return _aliases;
  }

  public static final MapTuple create() {
    return new MapTuple();
  }

  public static MapTuple create(String k, Object v) {
    return create().put(k,v);
  }
  
  public static MapTuple create(JSONObject o) {
    MapTuple m = new MapTuple();
    for(Object key : o.keySet()) {
      m.put((String) key, o.get(key));
    }
    return m;
  }

  @Override
  public long getByteSize() {
    return this.getApproxMemSize();
  }
  
  
  
}
