package grandmotherbrain.flow.operations;

import grandmotherbrain.flow.StateMachine;

import java.util.EnumSet;
import java.util.Set;

public enum AggregationState implements StateMachine<AggregationState> {

  INITIAL {
    // Initial state
    @Override
    public Set<AggregationState> predecessors() {
      return EnumSet.noneOf(AggregationState.class);
    }
  },

  STARTING {
    // Signals start of preparing phase;
    @Override
    public Set<AggregationState> predecessors() {
      return EnumSet.of(INITIAL, KILLED);
    }
  },
  STARTED {
    // Signals end of preparing phase
    @Override
    public Set<AggregationState> predecessors() {
      return EnumSet.of(STARTING);
    }
  },

  

  ACTIVE {
    // Means that tuples are getting processed
    public Set<AggregationState> predecessors() {return EnumSet.of(STARTED, IDLE);}
  },
  IDLE {
    // There has been no activity for a while, and we've set OURSELVEs into an idle state
    public Set<AggregationState> predecessors() {return EnumSet.of(STARTED, ACTIVE, SUSPECT);}
  },
  
  

  KILLING {
    // Kill signal has been sent, and we're to start cleaning up
    public Set<AggregationState> predecessors() {return EnumSet.complementOf(EnumSet.of(KILLED, ERROR));}
  },
  KILLED {
    // Kill signal has been sent, and we're to start cleaning up
    public Set<AggregationState> predecessors() {return EnumSet.of(KILLING);}
  },


  ERROR {
    // Set when a critical error occurs
    public Set<AggregationState> predecessors() {return EnumSet.complementOf(EnumSet.of(KILLING, KILLED));}
  },
  SUSPECT {
    // Set when loop errors exceed some threshold
    public Set<AggregationState> predecessors() {return EnumSet.of(ACTIVE);}
  },
  

}