package com.zillabyte.motherbrain.relational;

import java.util.concurrent.Callable;

import net.sf.json.JSONObject;

import org.apache.commons.lang.mutable.MutableBoolean;

import com.zillabyte.motherbrain.api.APIException;
import com.zillabyte.motherbrain.api.RelationsHelper;
import com.zillabyte.motherbrain.flow.config.FlowConfig;
import com.zillabyte.motherbrain.utils.Utils;

public class APIRelationDefFactory extends RelationDefFactory {


  /**
   * 
   */
  private static final long serialVersionUID = 4377049071457885406L;

  @Override
  public JSONObject getFromAPI(final FlowConfig config, final String relationName, final ColumnDef... columns) throws APIException, InterruptedException, RelationException {
    try { 
        
      final MutableBoolean relationAbsent = new MutableBoolean(false);
      
      // There's a race condition here, and we're lazy so just retry a few times... 
      JSONObject mappings = Utils.retry(3, new Callable<JSONObject>() {
        @Override
        public JSONObject call() throws Exception {
  
          relationAbsent.setValue(false);
          JSONObject ret = RelationsHelper.instance().getRelationConfig(config, relationName);
  
          if (ret.containsKey("error")) {
            relationAbsent.setValue(true);
            // Doctor, we need a relation, and fast!
            ret = RelationsHelper.instance().postRelationConfig(config, relationName, columns);
            if (ret.containsKey("error")) {
              // Alas, it was not to be.
              throw new APIException("api returned error! " + ret.toString());
            }
          }
          
          return ret;
        }
      });
      

      return mappings;
    } catch(Exception e) {
      throw new APIException(e);
    }
  }
}
