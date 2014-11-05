package grandmotherbrain.flow.operations;

import grandmotherbrain.flow.StateMachine;

import java.util.EnumSet;
import java.util.Set;

public enum SourceState implements StateMachine<SourceState> {

  INITIAL {
    // Initial state
    public Set<SourceState> predecessors() {return EnumSet.noneOf(SourceState.class);}
  },
  
  STARTING {
    // Signals start of preparing phase; 
    public Set<SourceState> predecessors() {return EnumSet.of(INITIAL, KILLED);}
  },
  STARTED {
    // Signals end of preparing phase
    public Set<SourceState> predecessors() {return EnumSet.of(STARTING);}
  },
  
  
  EMITTING {
    // Tells the spout to start emitting stuff
    public Set<SourceState> predecessors() {return EnumSet.of(STARTED, IDLE, PAUSED, WAITING_FOR_NEXT_CYCLE);}
  },
  
  PAUSING {
    // Tells the spout to halt emitting, transition once all tuples have been sent to the output collector
    public Set<SourceState> predecessors() {return EnumSet.of(STARTED, EMIT_COMPLETE, IDLE, WAITING_FOR_NEXT_CYCLE, EMITTING, SUSPECT);}
  },
  
  PAUSED {
    // Tells the spout to halt emitting but maintain state
    public Set<SourceState> predecessors() {return EnumSet.of(PAUSING);}
  },
  
  EMIT_COMPLETE {
    // Signals that the spout is done emitting
    public Set<SourceState> predecessors() {return EnumSet.of(EMITTING, SUSPECT);}
  },
  
  EMIT_COMPLETE_ACKED {
    // Signals that the spout is done emitting
    public Set<SourceState> predecessors() {return EnumSet.of(EMITTING, EMIT_COMPLETE_ACKED);}
  },
  
  WAITING_FOR_NEXT_CYCLE {
    public Set<SourceState> predecessors() {return EnumSet.of(EMIT_COMPLETE, EMIT_COMPLETE_ACKED);}
  },
  
  
  IDLE {
    public Set<SourceState> predecessors() {return EnumSet.of(STARTED, EMITTING, SUSPECT);}
  },
  
  KILLING {
    // Kill signal has been sent, and we're to start cleaning up
    public Set<SourceState> predecessors() {return EnumSet.complementOf(EnumSet.of(KILLED, ERROR));}
  },
  KILLED {
    // Kill signal has been sent, and we're to start cleaning up
    public Set<SourceState> predecessors() {return EnumSet.of(KILLING);}
  },

  
  ERROR {
    // Set when a critical error occurs
    public Set<SourceState> predecessors() {return EnumSet.complementOf(EnumSet.of(KILLING, KILLED));}
  },
  SUSPECT {
    // Set when loop errors exceed some threshold
    public Set<SourceState> predecessors() {return EnumSet.of(EMITTING);}
  },

  
}
