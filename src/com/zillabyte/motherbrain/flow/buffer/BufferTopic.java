package com.zillabyte.motherbrain.flow.buffer;

import java.io.Serializable;

public class BufferTopic implements Serializable {

  private static final long serialVersionUID = -791867054100136001L;
  private String _name;
  private Integer _cycle; 
  
  public BufferTopic(String name, Integer cycle) {
    _name = name;
    _cycle = cycle;
  }
  
  public String getBufferName() {
    return _name;
  }
  
  public Integer getCycle() {
    return _cycle;
  }

  public String getFullName() {
    return getFullNameFor(_name, _cycle);
  }
  
  @Override
  public String toString() {
    return getFullName();
  }

  public static String getFullNameFor(String buffer, int cycle) {
    return buffer + "_cycle_" + cycle;
  }
  
}
