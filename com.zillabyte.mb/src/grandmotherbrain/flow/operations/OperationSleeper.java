package grandmotherbrain.flow.operations;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

public class OperationSleeper implements Serializable {

  private static final long serialVersionUID = -6034452657920383472L;
  private long _sleepUntil = 0L;

  public boolean isShouldSleep() {
    return (_sleepUntil > System.currentTimeMillis());
  }
  
  public synchronized void sleepFor(long duration, TimeUnit unit) {
    long curTime = System.currentTimeMillis();
    _sleepUntil = Math.max(_sleepUntil, curTime + TimeUnit.MILLISECONDS.convert(duration, unit));
  }

  /**
   * 
   * @param duration in Millis
   */
  public void sleepFor(long duration) {
    sleepFor(duration, TimeUnit.MILLISECONDS);
  }
  
  public void sleepUntil(long systemMillis){
    _sleepUntil = systemMillis;
  }

  public void sleepForever() {
    sleepFor(100, TimeUnit.DAYS);
  }
}
