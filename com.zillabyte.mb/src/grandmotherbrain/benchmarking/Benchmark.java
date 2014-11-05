package grandmotherbrain.benchmarking;

import grandmotherbrain.universe.Universe;

import java.io.Serializable;

public abstract class Benchmark implements Serializable {

  
  private static final long serialVersionUID = -1676421550497516186L;
  
  private static Benchmark _instance = null;

  public abstract void begin(final String name);
  
  public abstract void end(final String name);
  
  
  /**
   * 
   * @return
   */
  public synchronized static Benchmark instance() {
    if (_instance == null) {
      _instance = Universe.instance().benchmarkFactory().create();
    }
    return _instance;
  }
  
  
  /***
   * 
   * @param name
   */
  public static void markBegin(final String name) {
    instance().begin(name);
  }
  
  public static void markEnd(final String name) {
    instance().end(name);
  }
  
}
