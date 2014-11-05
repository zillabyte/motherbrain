package com.zillabyte.motherbrain.flow;

import java.util.EnumSet;
import java.util.Set;



public enum FlowState implements StateMachine<FlowState> {


  INITIAL {
    // Initial state
    @Override
    public Set<FlowState> predecessors() {return EnumSet.noneOf(FlowState.class);}
  },
  
  
  STARTING {
    // Set when we start preparing 
    @Override
    public Set<FlowState> predecessors() {return EnumSet.of(INITIAL, KILLED, RECOVERING);}
  },
  STARTED {
    // After the preparation phase
    @Override
    public Set<FlowState> predecessors() {return EnumSet.of(STARTING, RECOVERING);}
  },
  

  RUNNING {
    // Tells the flow to start running
    @Override
    public Set<FlowState> predecessors() {return EnumSet.of(STARTED, WAITING_FOR_NEXT_CYCLE, IDLE, RECOVERING, PAUSED);}
  },
  
  PAUSING {
    // Tells the flow to start running
    @Override
    public Set<FlowState> predecessors() {return EnumSet.of(RUNNING, WAITING_FOR_NEXT_CYCLE, IDLE);}
  },
  PAUSED {
    // Tells the flow to start running
    @Override
    public Set<FlowState> predecessors() {return EnumSet.of(PAUSING);}
  },
  CYCLE_COMPLETE {
    // A short state after we're done running, but before we officially wait for the enxt cycle
    @Override
    public Set<FlowState> predecessors() {return EnumSet.of(RUNNING, RECOVERING);}
  }, 
  
  WAITING_FOR_NEXT_CYCLE {
    // set after the 'CYCLE_COMPLETED' has been acknowledged
    @Override
    public Set<FlowState> predecessors() {return EnumSet.of(CYCLE_COMPLETE, RECOVERING);}
  },
  
  IDLE {
    // set after all operations in an rpc are idled
    @Override
    public Set<FlowState> predecessors() {return EnumSet.of(RUNNING);}
  },
   

  KILLING {
    // Kill signal has been sent, and we're to start cleaning up
    @Override
    public Set<FlowState> predecessors() {return EnumSet.complementOf(EnumSet.of(KILLED, RETIRING, RETIRED, ERRORING, ERROR));}
  },
  KILLED {
    // Kill cleanup is done
    @Override
    public Set<FlowState> predecessors() {return EnumSet.of(KILLING, ERROR);}
  },
  
  
  
  RETIRING {
    // Retire signal has been sent, and we're to start cleaning up
    @Override
    public Set<FlowState> predecessors() {return EnumSet.of(WAITING_FOR_NEXT_CYCLE, IDLE);}
  },
  RETIRED {
    // Retire cleanup is done
    @Override
    public Set<FlowState> predecessors() {return EnumSet.of(RETIRING);}
  },

  ERRORING {
    @Override
    public Set<FlowState> predecessors() {return EnumSet.complementOf(EnumSet.of(ERROR, KILLING, KILLED, RETIRING, RETIRED));}
  },
  ERROR {
    // Set when there's been an unhanlded error
    @Override
    public Set<FlowState> predecessors() {return EnumSet.of(ERRORING);}
  },
  

  RECOVERING {
    // When the FlowInstance has been recovered
    @Override
    public Set<FlowState> predecessors() {return EnumSet.noneOf(FlowState.class);}
  },
  
}
