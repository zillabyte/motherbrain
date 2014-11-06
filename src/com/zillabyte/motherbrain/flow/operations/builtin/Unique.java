package com.zillabyte.motherbrain.flow.operations.builtin;

import java.nio.charset.Charset;
import java.util.Map;

import net.sf.json.JSONObject;

import com.google.common.collect.Maps;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;
import com.zillabyte.motherbrain.flow.Fields;
import com.zillabyte.motherbrain.flow.MapTuple;
import com.zillabyte.motherbrain.flow.collectors.OutputCollector;
import com.zillabyte.motherbrain.flow.config.OperationConfig;
import com.zillabyte.motherbrain.flow.operations.Function;
import com.zillabyte.motherbrain.flow.operations.OperationException;
import com.zillabyte.motherbrain.flow.operations.multilang.operations.MultilangHandler;
import com.zillabyte.motherbrain.universe.Config;


public class Unique extends Function {

 
  private static final long serialVersionUID = -1116914507247257811L;
  private Integer _expectedSize = Config.getOrDefault("unique.bloom.filter.size", 5_000_000);
  private transient Map<Object, BloomFilter<MapTuple>> _filters = null;
  private Fields _uniqueFields = null;
  

  public Unique(String name, OperationConfig config, Fields fields) {
    super(name, config);
    if (fields != null && fields.size() > 0) {
      _uniqueFields = fields;
      setIncomingRouteByFields(fields);
    }
  }
  
  
  
  public Unique(JSONObject node) {
    this(node.getString("name"), MultilangHandler.getConfig(node), new Fields(node.getJSONArray("group_fields")));
    if (node.has("config") && node.getJSONObject("config").has("expected_size")) {
      _expectedSize = node.getJSONObject("config").getInt("expected_size");
    }
  }

  
  private BloomFilter<MapTuple> getFilter(Object batch) {
    if (_filters.containsKey(batch) == false) {
      Funnel<MapTuple> funnel = new Funnel<MapTuple>() {
        @Override
        public void funnel(MapTuple from, PrimitiveSink into) {
          if (_uniqueFields == null) {
            into.putString(from.values().toString(), Charset.defaultCharset());
          } else {
            for(String f : _uniqueFields) {
              into.putString(from.get(f).toString(), Charset.defaultCharset());
            }
          }
        }
      };
      logger().info("Creating unique filter with max expected capacity of: " + _expectedSize);
      _filters.put(batch, BloomFilter.create(funnel, _expectedSize));
    }
    return _filters.get(batch);
  }

  /***
   * 
   */
  @SuppressWarnings("serial")
  @Override
  public void prepare() {
    _filters = Maps.newHashMap();
  }

  

  @Override
  public int getMaxParallelism() {
    if (this._uniqueFields == null) {
      return 1;
    } else {
      return super.getMaxParallelism();
    }
  }

  @Override
  public void onThisBatchCompleted(Object batchId) {
    _filters.remove(batchId);
  }

  @Override
  protected void process(MapTuple t, OutputCollector c) throws OperationException, InterruptedException {
    BloomFilter<MapTuple> filter = getFilter(c.getCurrentBatch());
    if (filter.mightContain(t)) {
      // Do nothing... 
    } else {
      filter.put(t);
      c.emit(t);
    }
  }
  
  

}
