package com.zillabyte.motherbrain.flow.operations;

import java.util.EnumSet;
import java.util.Set;

import com.zillabyte.motherbrain.flow.StateMachine;


public enum FunctionState implements StateMachine<FunctionState> {

  INITIAL {
    // Initial state
    public Set<FunctionState> predecessors() {return EnumSet.noneOf(FunctionState.class);}
  },


  STARTING {
    // Signals start of preparing phase; 
    public Set<FunctionState> predecessors() {return EnumSet.of(INITIAL, KILLED);}
  },
  STARTED {
    // Signals end of preparing phase
    public Set<FunctionState> predecessors() {return EnumSet.of(STARTING);}
  },


  ACTIVE {
    // Means that tuples are getting processed
    public Set<FunctionState> predecessors() {return EnumSet.of(STARTED, IDLE, PAUSED);}
  },
  PAUSING {
    // Pause the operation (do not retrieve more tuples for processing)
    public Set<FunctionState> predecessors() {return EnumSet.of(STARTED, ACTIVE, SUSPECT, IDLE);}
  },
  PAUSED {
    // Pause the operation (do not retrieve more tuples for processing)
    public Set<FunctionState> predecessors() {return EnumSet.of(PAUSING, IDLE);}
  },

  IDLE {
    // There has been no activity for a while, and we've set OURSELVEs into an idle state
    public Set<FunctionState> predecessors() {return EnumSet.of(STARTED, ACTIVE, SUSPECT, PAUSING);}
  },
  KILLING {
    // Kill signal has been sent, and we're to start cleaning up
    public Set<FunctionState> predecessors() {return EnumSet.complementOf(EnumSet.of(KILLED, ERROR));}
  },
  KILLED {
    // Kill signal has been sent, and we're to start cleaning up
    public Set<FunctionState> predecessors() {return EnumSet.of(KILLING);}
  },


  ERROR {
    // Set when a critical error occurs
    public Set<FunctionState> predecessors() {return EnumSet.complementOf(EnumSet.of(KILLING, KILLED));}
  },
  SUSPECT {
    // Set when loop errors exceed some threshold
    public Set<FunctionState> predecessors() {return EnumSet.of(ACTIVE);}
  },

}
