package com.zillabyte.motherbrain.flow;

import java.util.Set;

public interface StateMachine<T> {
  
  /**
   * returns a set of allowed predecessors for this state
   * @return
   */
  public abstract Set<T> predecessors();
  
  
}