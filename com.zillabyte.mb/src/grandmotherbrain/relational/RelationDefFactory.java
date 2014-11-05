package grandmotherbrain.relational;

import grandmotherbrain.api.APIException;
import grandmotherbrain.flow.config.FlowConfig;

import java.io.Serializable;
import java.util.List;

import net.sf.json.JSONObject;

import org.eclipse.jdt.annotation.NonNullByDefault;

@NonNullByDefault
public abstract class RelationDefFactory implements Serializable {

  /**
   * 
   */
  private static final long serialVersionUID = -6366216282828598827L;

  public abstract JSONObject getFromAPI(FlowConfig config, String relationName, ColumnDef... columns)
      throws APIException, RelationException, InterruptedException;

  public final JSONObject getFromAPI(FlowConfig config, String relationName, List<ColumnDef> columns) throws APIException, RelationException, InterruptedException {
    final ColumnDef[] columnArray = columns.toArray(new ColumnDef[] {});
    return getFromAPI(config, relationName, columnArray);
  }
}
