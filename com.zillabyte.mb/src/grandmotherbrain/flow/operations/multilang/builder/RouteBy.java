package grandmotherbrain.flow.operations.multilang.builder;

import java.util.List;

import net.sf.json.JSONObject;

import com.google.common.collect.Lists;

public class RouteBy {

  private List<String> _fields = Lists.newArrayList();

  public RouteBy(JSONObject node) {
    for(Object o : node.getJSONObject("config").getJSONArray("fields")) {
      _fields.add(o.toString());
    }
  }

  public List<String> getFields() {
    return _fields;
  }

}
