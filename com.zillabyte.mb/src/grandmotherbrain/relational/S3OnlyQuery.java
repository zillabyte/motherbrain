package grandmotherbrain.relational;


import grandmotherbrain.flow.sourcefromrelation.ShardType;
import net.sf.json.JSONObject;

import org.eclipse.jdt.annotation.NonNullByDefault;

public class S3OnlyQuery extends AliasedQuery {

  private static final long serialVersionUID = -7267594005164663029L;
  
  private JSONObject _settings;

  public @NonNullByDefault S3OnlyQuery(JSONObject settings) {
    super("", columnsFor(settings));
    _settings = settings;
  }

  public String bucket() {
    return _settings.getString("s3_bucket");
  }
  
  public String prefix() {
    return _settings.getString("s3_root");
  }

  @Override
  public ShardType shardType() {
    if(_settings.containsKey("shard_type")){
      String shardType = _settings.getString("shard_type");
      if(shardType != null){
        if("csv".equals(shardType) || "csv_redshift".equals(shardType))
          return ShardType.CSV_REDSHIFT;
        if("csv_collectors".equals(shardType))
          return ShardType.CSV_COLLECTORS;
        else if ("json".equals(shardType))
          return ShardType.JSON;
      }
    }
    return ShardType.JSON;
  }
    

}
