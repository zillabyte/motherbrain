package grandmotherbrain.utils;

import grandmotherbrain.flow.operations.OperationLogger;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.RateLimiter;

public class MeteredLog {

  private static Cache<Object, RateLimiter> _cache = CacheBuilder.newBuilder()
    .concurrencyLevel(4)
    .weakKeys()
    .maximumSize(100)
    .expireAfterWrite(10, TimeUnit.MINUTES)
    .build();
  
  
  /***
   * 
   * @param log
   * @param message
   * @param maxEvery
   * @param warmupPeriod
   * @param unit
   * @return
   */
  public static RateLimiter getLimiter(final Object log, final long maxEvery, final long warmupPeriod, final TimeUnit unit) {
    try { 
      return _cache.get(log, new Callable<RateLimiter>() {
        @Override
        public RateLimiter call() throws Exception {
          double rate = 1.0 / TimeUnit.SECONDS.convert(maxEvery, unit);
          return RateLimiter.create(rate, warmupPeriod, TimeUnit.MILLISECONDS);
        }
      });
    } catch(Exception e) {
      return Utils.propagate(e);
    }
  }
  
  
  /***
   * 
   * @param log
   * @param maxEvery
   * @param warmupPeriod
   * @param unit
   * @return
   */
  public static boolean permit(Object log, long maxEvery, long warmupPeriod, TimeUnit unit) {
    return getLimiter(log, maxEvery, warmupPeriod, unit).tryAcquire(0, unit);
  }
  
  
  /***
   * 
   * @param log
   * @param message
   * @param maxEvery
   * @param warmupPeriod
   * @param unit
   */
  public static void info(Logger log, String message, long maxEvery, long warmupPeriod, TimeUnit unit) {
    if (permit(log, maxEvery, warmupPeriod, unit)) {
      log.log(MeteredLog.class.getName(), Level.INFO, message, null);
    }
  }
  
  public static void info(Logger log, String message, long maxEvery, long warmupPeriod) {
    info(log, message, maxEvery, warmupPeriod, TimeUnit.MILLISECONDS);
  }
  
  public static void info(Logger log, String message, long maxEvery) {
    info(log, message, maxEvery, maxEvery);
  }
  
  public static void info(Logger log, String message) {
    info(log, message, 1000L * 30);
  }
  
  
  
  
  
  public static void info(Log4jWrapper log, String message, long maxEvery, long warmupPeriod, TimeUnit unit) {
    if (permit(log, maxEvery, warmupPeriod, unit)) {
      log.info(message);
    }
  }
  
  public static void info(Log4jWrapper log, String message, long maxEvery, long warmupPeriod) {
    info(log, message, maxEvery, warmupPeriod, TimeUnit.MILLISECONDS);
  }
  
  public static void info(Log4jWrapper log, String message, long maxEvery) {
    info(log, message, maxEvery, maxEvery);
  }
  
  public static void info(Log4jWrapper log, String message) {
    info(log, message, 1000L * 30);
  }
  
  
  
  public static void info(OperationLogger log, String message, long maxEvery, long warmupPeriod, TimeUnit unit) {
    if (permit(log, maxEvery, warmupPeriod, unit)) {
      log.info(message);
    }
  }
  
  public static void info(OperationLogger log, String message, long maxEvery, long warmupPeriod) {
    info(log, message, maxEvery, warmupPeriod, TimeUnit.MILLISECONDS);
  }
  
  public static void info(OperationLogger log, String message, long maxEvery) {
    info(log, message, maxEvery, maxEvery);
  }
  
  public static void info(OperationLogger log, String message) {
    info(log, message, 1000L * 30);
  }
  
  
  
  
}
