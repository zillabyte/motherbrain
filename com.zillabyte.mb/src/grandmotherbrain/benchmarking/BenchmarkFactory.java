package grandmotherbrain.benchmarking;

import java.io.Serializable;

@SuppressWarnings("serial")
public abstract class BenchmarkFactory implements Serializable {
  
  public abstract Benchmark create();
  
  
  public static class Noop extends BenchmarkFactory {
    @Override
    public Benchmark create() {
      return new NoopBenchmarker();
    }
  }
  
  
  public static class Stderr extends BenchmarkFactory {
    @Override
    public Benchmark create() {
      return new StderrBenchmarker();
    }
  }
  
  
  
  public static class Graphite extends BenchmarkFactory {
    @Override
    public Benchmark create() {
      return new GraphiteBenchmarker();
    }
  }
  
  
  public static class MovingLogger extends BenchmarkFactory {
    @Override
    public Benchmark create() {
      return new MovingBenchmarker();
    }
  }
  

}
