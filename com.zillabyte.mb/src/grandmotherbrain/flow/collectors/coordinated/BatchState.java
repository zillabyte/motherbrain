package grandmotherbrain.flow.collectors.coordinated;

public enum BatchState {
  EMITTING,    // Normal, emitting mode
  COMPLETING,  // Ready to complete, once all acks/fails have happened
  COMPLETED,   // THIS batch is compeleted
  ACKED,       // Downstream ops have ack'ed the batch-complete
}
