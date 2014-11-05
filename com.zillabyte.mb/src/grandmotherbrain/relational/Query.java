package grandmotherbrain.relational;


import grandmotherbrain.flow.sourcefromrelation.ShardType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.eclipse.jdt.annotation.NonNull;
import org.eclipse.jdt.annotation.NonNullByDefault;

public class Query implements Serializable {

  /**
   * 
   */
  private static final long serialVersionUID = 7577193370942882125L;
  protected final @NonNull JSONArray _aliases;
  private final @NonNull String _sql;
  
  public Query(final @NonNull String sql, final @NonNull JSONArray columns) {
    _sql = sql;
    _aliases = columns;
  }
  

  public String sql() {
    return _sql;
  }

  /**
   * Sql queries don't have types that we know of
   * @return
   */
  public @NonNullByDefault List<ColumnDef> valueColumns() {
    List<ColumnDef> list = new ArrayList<>();
    for(int i=0;i<_aliases.size();i++) {
      JSONObject o = _aliases.getJSONObject(i);
      ColumnDef c = new ColumnDef("v" + i, DataType.UNKNOWN, o.getString("alias"));
      list.add(c);
    }
    return list;
  }
  

  public String getAliasOrConcreteName(String concreteName) {
    if (_aliases.size() != 0) {
      for(Object raw : _aliases) {
        JSONObject obj = (JSONObject) raw;
        if (obj.getString("concrete_name").equals(concreteName)) {
        
          // TODO: make compatible with single-alias 
          String alias = obj.optString("alias", null);
          if(alias != null) {
            return alias;
          }
          return obj.getString("concrete_name");
          
        }
      }
    } 
    return concreteName;
  }
  
  
  public long batchSize = -1;
  
  /**********************/
  
  
  public JSONArray getAliases() {
    return _aliases;
  }

  public @NonNullByDefault ShardType shardType() {
    return ShardType.JSON;
  }

  static final @NonNullByDefault JSONArray columnsFor(final JSONObject settings) {
    final JSONArray columnArray = settings.getJSONArray("columns");
    if (columnArray == null) {
      throw new NullPointerException("columns should not be null!");
    }
    return columnArray;
  }
}
