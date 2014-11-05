package grandmotherbrain.relational;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public abstract class AliasedQuery extends Query {
  
  private static final long serialVersionUID = -3332999868524375404L;

  public AliasedQuery(String sql, JSONArray columns) {
    super(sql, columns);
  }

  /**
   * Queries that we pre-define (s3, buffer) have aliases set in advance
   */
  @Override
  public final List<ColumnDef> valueColumns() {
    List<ColumnDef> cols = new ArrayList<>();
    for(Object r : _aliases) {
      JSONObject col = (JSONObject) r;
      Collection<?> aliases = JSONArray.toCollection(col.getJSONArray("aliases"));
      if (aliases == null) {
        throw new NullPointerException("aliases collection should not be null!");
      }
      final String[] aliasesArray = aliases.toArray(new String[] {});
      assert (aliasesArray != null);
      final String name = col.getString("name");
      if (name == null) {
        throw new NullPointerException("name should exist or be non-null!");
      }
      final String type = col.getString("type");
      if (type == null) {
        throw new NullPointerException("type for column " + name + " should exist or be non-null!");
      }
      final DataType dataType = DataType.valueOf(type.toUpperCase());
      /*
       * If valueOf fails, it will throw an IllegalArgumentException.
       */
      assert (dataType != null);
      cols.add(new ColumnDef(name, dataType, aliasesArray));
    }
    return cols;
  }
  
}
