package com.zillabyte.motherbrain.relational;

import java.io.Serializable;
import java.util.List;

import net.sf.json.JSONObject;

import org.eclipse.jdt.annotation.NonNullByDefault;

import com.zillabyte.motherbrain.api.APIException;
import com.zillabyte.motherbrain.flow.config.FlowConfig;

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
