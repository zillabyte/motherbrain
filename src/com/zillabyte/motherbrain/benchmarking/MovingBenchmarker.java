package com.zillabyte.motherbrain.benchmarking;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ScheduledFuture;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.log4j.Logger;
import org.codehaus.plexus.util.StringUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.zillabyte.motherbrain.utils.Utils;

public class MovingBenchmarker extends Benchmark {

  /**
   * 
   */
  private static final long serialVersionUID = -1763550105106202667L;
  
  private static final int WINDOW_SIZE = 1000;
  private static final long REPORT_PERIOD = 1000L * 10;
  
  private static Logger _log = Utils.getLogger(MovingBenchmarker.class);
  private static Map<String, DescriptiveStatistics> _buffer = Maps.newHashMap();
  private static Map<String, Map<Thread, Long>> _starts = Maps.newHashMap();
  private static ScheduledFuture<?> _future = null;
  
  public MovingBenchmarker() {
    synchronized(MovingBenchmarker.class) {
      if (_future == null) {
        _future = Utils.timerFromPool(REPORT_PERIOD, new Runnable() {
          @Override
          public void run() {
            _log.info("-= BEGIN BENCHMARK REPORT =-");
            for(String s : StringUtils.split(createReport(), "\n")) {
              _log.info(s);
            }
            _log.info("-= END BENCHMARK REPORT =-");
          }
        });
      }
    }
  }
  
  

  
  /**
   * 
   * @param scope
   * @param name
   */
  @Override
  public void begin(String name) {
    getStart(name).put(Thread.currentThread(), System.currentTimeMillis());
  }
  
  
  private static Map<Thread, Long> getStart(String name) {
    if (_starts.containsKey(name) == false) {
      _starts.put(name, new HashMap<Thread, Long>());
    }
    return _starts.get(name);
  }

  
  /***
   * 
   * @param scope
   * @param name
   */
  @Override
  public void end(String name) {
    Long start = getStart(name).get(Thread.currentThread());
    if (start == null) throw new IllegalStateException("benchmark.end() called without start(): " + name);
    if (_buffer.containsKey(name) == false) {
      _buffer.put(name, new DescriptiveStatistics(WINDOW_SIZE));
    }
    _buffer.get(name).addValue(System.currentTimeMillis() - start);
  }

  
  
  public String createReport() {
    
    int rightPad = 50;
    StringBuilder sb = new StringBuilder();
    
    ArrayList<Entry<String, DescriptiveStatistics>> list = Lists.newArrayList(_buffer.entrySet());
    Collections.sort(list, new Comparator<Entry<String, DescriptiveStatistics>>() {
      @Override
      public int compare(Entry<String, DescriptiveStatistics> o1, Entry<String, DescriptiveStatistics> o2) {
        return o1.getKey().compareTo(o2.getKey());
      }
    });
    
    for(Entry<String, DescriptiveStatistics> e : list) {
      
      DescriptiveStatistics d = e.getValue();
      
      sb.append(StringUtils.rightPad("* " + e.getKey() + " (" + d.getWindowSize() + ")", rightPad, "."));
      sb.append("mean: " + d.getMean() + "ms\n");
      
      sb.append(StringUtils.rightPad("", rightPad));
      sb.append("max:  " + d.getMax() + "ms\n");
      
      sb.append(StringUtils.rightPad("", rightPad));
      sb.append("min:  " + d.getMin() + "ms\n");
      
    }
    
    return sb.toString();
  }

}
