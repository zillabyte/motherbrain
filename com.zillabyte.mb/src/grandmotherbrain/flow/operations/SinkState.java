package grandmotherbrain.flow.operations;

import grandmotherbrain.flow.StateMachine;

import java.util.EnumSet;
import java.util.Set;


public enum SinkState implements StateMachine<SinkState> {
  
  INITIAL {
    // Initial state
    public Set<SinkState> predecessors() {return EnumSet.noneOf(SinkState.class);}
  },
  
  
  STARTING {
    // Signals start of preparing phase; 
    public Set<SinkState> predecessors() {return EnumSet.of(INITIAL, KILLED);}
  },
  STARTED {
    // Signals end of preparing phase
    public Set<SinkState> predecessors() {return EnumSet.of(STARTING);}
  },
  
  
  ACTIVE {
    // Means that tuples are getting processed
    public Set<SinkState> predecessors() {return EnumSet.of(STARTED, PAUSED, IDLE);}
  },
  IDLE {
    // There has been no activity for a while, and we've set OURSELVEs into an idle state
    public Set<SinkState> predecessors() {return EnumSet.of(STARTED, ACTIVE, SUSPECT, PAUSING);}
  },

  PAUSING {
    // Tells the spout to halt emitting, transition once all tuples have been sent to the output collector
    public Set<SinkState> predecessors() {return EnumSet.of(STARTED, ACTIVE, SUSPECT, IDLE);}
  },
  PAUSED {
    // The sink is alive but not accepting tuples
    public Set<SinkState> predecessors() {return EnumSet.of(PAUSING, IDLE);}
  },
  KILLING {
    // Kill signal has been sent, and we're to start cleaning up
    public Set<SinkState> predecessors() {return EnumSet.complementOf(EnumSet.of(KILLED, ERROR));}
  },
  KILLED {
    // Kill signal has been sent, and we're to start cleaning up
    public Set<SinkState> predecessors() {return EnumSet.of(KILLING);}
  },

  
  ERROR {
    // Set when a critical error occurs
    public Set<SinkState> predecessors() {return EnumSet.complementOf(EnumSet.of(KILLING, KILLED));}
  },
  SUSPECT {
    // Set when loop errors exceed some threshold
    public Set<SinkState> predecessors() {return EnumSet.of(ACTIVE);}
  },
  
}
