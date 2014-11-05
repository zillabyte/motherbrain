package grandmotherbrain.utils.backoff;

import grandmotherbrain.utils.SerializableMonitor;

import java.io.Serializable;

public abstract class BackoffTicker implements Serializable {
  
  private static final long serialVersionUID = -700196649719730495L;
  public static final long DEFAULT_INITIAL_VALUE = 0L;
  public static final long DEFAULT_UPPER_BOUND = Long.MAX_VALUE;

  long _counter;
  final SerializableMonitor counterMonitor;
  final long _upperBound;

  BackoffTicker(long counter, long upperBound) {
    this._counter = counter;
    this._upperBound = upperBound;
    this.counterMonitor = new SerializableMonitor();
  }

  public BackoffTicker() {
    this(DEFAULT_INITIAL_VALUE, DEFAULT_UPPER_BOUND);
  }

  /***
   * 
   * @param upperBound
   */
  public BackoffTicker(long upperBound) {
    this(DEFAULT_INITIAL_VALUE, upperBound);
  }

  /**
   * This is NOT thread-safe and may return an inaccurate count.
   */
  public long counter() {
    return _counter;
  }

  protected abstract boolean handleTick(long counter);

  public boolean tick() {
    synchronized (counterMonitor) {
      this._counter++;
      return (_counter % _upperBound == 0) || handleTick(_counter);
    }
  }
}
