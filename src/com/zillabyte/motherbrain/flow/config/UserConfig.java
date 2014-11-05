package com.zillabyte.motherbrain.flow.config;

import java.io.Serializable;
import java.util.Map;

import net.sf.json.JSONObject;

import com.google.common.collect.Maps;
import com.zillabyte.motherbrain.utils.JSONUtil;


/***
 * Contains env variables for the flow.  Thins like auth_token, etc 
 * @author jake
 *
 */
@SuppressWarnings("unchecked")
public class UserConfig implements Serializable {

  private static final long serialVersionUID = -2121354184495729948L;
  protected Map<String,Object> _settings = Maps.newHashMap();
  
  
  /***
   * 
   */
  public UserConfig() {
  }
  
  

  public boolean containsKey(String key) {
    return _settings.containsKey(key);
  }
  
  
  
  /****
   * 
   * @param key
   * @param def
   * @return
   */
  public <T> T get(String key, T def) {
    if (_settings.containsKey(key)) {
      return (T)_settings.get(key); 
    } else {
      return def;
    }
  }
  
  
  /***
   * 
   * @param key
   * @return
   */
  public <T> T get(String key) {
    return get(key, null);
  }
  
  
  /***
   * 
   * @param key
   * @param val
   * @return
   */
  public <T extends UserConfig> T set(String key, Object val) {
    _settings.put(key,  val);
    return (T)this;
  }
  
  
  /***
   * 
   * @param map
   * @return
   */
  public <T extends UserConfig> T setAll(Map<String,Object> map) {
    _settings.putAll(map);
    return (T)this;
  }
  
  

  public <T extends UserConfig> T setAll(UserConfig config) {
    _settings.putAll(config._settings);
    return (T)this;
  }

  

  public UserConfig mergeWith(Map<String, Object> conf) {
    UserConfig c = new UserConfig();
    c._settings.putAll(this._settings);
    c._settings.putAll(conf);
    return c;
  }

  

  

  public JSONObject toJSON() {
    return JSONUtil.toJSON(_settings);
  }

    
  
}
