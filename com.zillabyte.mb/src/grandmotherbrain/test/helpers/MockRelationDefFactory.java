package grandmotherbrain.test.helpers;

import grandmotherbrain.flow.config.FlowConfig;
import grandmotherbrain.relational.ColumnDef;
import grandmotherbrain.relational.RelationDefFactory;
import grandmotherbrain.universe.Universe;
import grandmotherbrain.utils.JSONUtil;
import net.sf.json.JSONObject;
import net.sf.json.util.JSONBuilder;
import net.sf.json.util.JSONStringer;

public class MockRelationDefFactory extends RelationDefFactory {

  /**
   * 
   */
  private static final long serialVersionUID = 1849658477874210226L;

  @Override
  public JSONObject getFromAPI(FlowConfig config, String relationName, ColumnDef... columns) {

     JSONBuilder builder = new JSONStringer();
     builder.object()
       .key("buffer_settings").object()
         .key("topic").value(relationName)
         .key("source").object()
           .key("type").value("s3")
           .key("retry").value(0)
           .key("config").object()
             .key("shard_path").value(Universe.instance().env() + "/" + relationName + "/")
             .key("shard_prefix").value("shard_")
             .key("bucket").value("buffer.zillabyte.com")
             .key("credentials").object()
               .key("secret").value("")
               .key("access").value("")
             .endObject()
           .endObject()
         .endObject()
       .endObject()
     .endObject();

     return JSONUtil.parseObj(builder.toString()); // Builder to JSONObject?
  }

}
