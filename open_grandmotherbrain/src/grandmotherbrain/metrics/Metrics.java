package grandmotherbrain.metrics;

public interface Metrics {

  public abstract void increment(String metric);

  public abstract void mark(String metric);

  
  
  
}