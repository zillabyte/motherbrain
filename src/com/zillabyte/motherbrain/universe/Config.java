package com.zillabyte.motherbrain.universe;

import java.io.Serializable;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.StringUtils;
import org.eclipse.jdt.annotation.NonNullByDefault;

@NonNullByDefault
public class Config implements Serializable {

  private static final long serialVersionUID = 2815004833459597124L;
  
  private final ConcurrentHashMap<String, Object> _props; // Note: no properties can be null

  public Config() {
    this(new Properties());
  }

  public Config(Properties properties) {
    _props = new ConcurrentHashMap<>();
    for(Entry<Object, Object> e : properties.entrySet()) {
      _props.put(e.getKey().toString(), e.getValue());
    }
  }
  
  public Config(Map<String, Object> properties) {
    _props = new ConcurrentHashMap<>(properties);
  }


  protected final <T extends Object> void setInternal(String key, T val) {
    _props.put(key, val);
  }
  
  
  public final boolean contains(String key) {
    return _props.containsKey(key);
  }

  public final <T extends Object> T get(String key, T defaultValue) {
    @SuppressWarnings("unchecked")
    final T value = (T) _props.get(key);
    return value == null ? defaultValue : value;
  }
  
  public final <T extends Object> T getOrException(String key) {
    if (_props.containsKey(key)) {
      @SuppressWarnings("unchecked")
      final T value = (T) _props.get(key);
      /*
       * Indeed should be impossible, as we don't allow nulls in
       */
      assert (value != null);
      return value;
    }
    throw new ExpectedConfigNotPresent();
  }
    
  
  
  /****
   * Syntatic sugar to take up less screen real estate
   */
  public static <T extends Object> T getOrDefault(String k, T v) {
    final Config c = Universe.instance().config();
    return c.get(k, v);
  }
  
  
  /***
   * 
   * @param k
   * @param v
   * @return
   */
  public static <T> void setDefault(String k, T v) {
    final Config c = Universe.instance().config();
    c.put(k, v);
  }

  
  
  /***
   * 
   */
  public static void reset() {
    Universe.instance().config()._props.clear();
  }

  public final synchronized <T extends Object> void put(String key, T val) {
    this._props.put(key, val);
  }



  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$\n");
    for(Entry<String, Object> e : this._props.entrySet()) {
      sb.append("$$  ");
      sb.append( StringUtils.rightPad(e.getKey(), 40));
      sb.append(" : ");
      sb.append(e.getValue());
      sb.append("\n");
    }
    sb.append("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$\n");
    final String string = sb.toString();
    assert (string != null);
    return string;
  }
  
  public void debug() {
    System.err.println(this.toString());
  }




}
