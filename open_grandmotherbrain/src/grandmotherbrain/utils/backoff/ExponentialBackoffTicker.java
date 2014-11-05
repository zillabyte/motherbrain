package grandmotherbrain.utils.backoff;

public class ExponentialBackoffTicker extends BackoffTicker {

  
  private static final long serialVersionUID = -1000254858589723349L;
  private long _nextTick = 1;
  
  public ExponentialBackoffTicker() {
    super();
  }
  
  public ExponentialBackoffTicker(long upperBound) {
    super(upperBound);
  }
  
  
  @Override
  protected final boolean handleTick(long counter) {
    /*
     * We happen to know this is protected by a synchronized block.
     */
    if (_nextTick == counter) {
      _nextTick = _nextTick * 2;
      return true;
    }
    return false;
  }

}
