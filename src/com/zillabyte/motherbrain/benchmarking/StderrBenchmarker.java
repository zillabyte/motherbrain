package com.zillabyte.motherbrain.benchmarking;

import java.util.Map;

import org.apache.log4j.Logger;

import com.google.common.collect.Maps;

public class StderrBenchmarker extends Benchmark {

  
  private static final long serialVersionUID = -724934236310162049L;

  private static Logger _log = Logger.getLogger(StderrBenchmarker.class);
  
  private Map<String, Long> _map = Maps.newHashMap();
  
  
  
  /**
   * 
   * @param scope
   * @param name
   */
  @Override
  public void begin(String name) {
    //System.err.println("Benchmark start: " + name);
    if (_map.containsKey(name)) {
      _log.warn("benchmark key already existis for thread: " + name);
      return;
    }
    _map.put(name, System.currentTimeMillis());
  }

  
  /***
   * 
   * @param scope
   * @param name
   */
  @Override
  public void end(String name) {
    Long start = _map.remove(name);
    if (start == null) {
      _log.error("banchmark '" + name + "' did not have a corresponding begin()" );
      return;
    }
    long delta = System.currentTimeMillis() - start;
    System.err.println("Benchmark end. '" + name + "' took: " + (delta/1000.0) + "s");
  }

}
