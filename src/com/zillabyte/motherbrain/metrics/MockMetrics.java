package com.zillabyte.motherbrain.metrics;

import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.Maps;

public class MockMetrics implements Metrics {

  private HashMap<String, Integer> _counters;
  private HashMap<String, Integer> _meters;

  
  public MockMetrics() {
    _counters = Maps.newHashMap();
    _meters = Maps.newHashMap();
  }
  
  private static String keyify(String metric) {
    return metric;
  }
  
  @Override
  public void mark(String metric) {
    inc(_meters, metric);
  }

  @Override
  public void increment(String metric) {
    inc(_counters, metric);
  }
  
  
  private static void inc(Map<String, Integer> map, String metric) {
    if (map.containsKey(metric)) {
      map.put(metric, Integer.valueOf(map.get(metric).intValue() + 1));
    } else {
      map.put(metric, Integer.valueOf(1));
    }
  }
  
  public Integer getCounter(String metric) {
    return _counters.get(keyify(metric));
  }
  
  public Integer getMeter(String metric) {
    return _meters.get(keyify(metric));
  }

}
