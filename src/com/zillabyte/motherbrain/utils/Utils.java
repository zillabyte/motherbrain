package com.zillabyte.motherbrain.utils;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import javax.annotation.RegEx;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang.mutable.MutableObject;
import org.apache.log4j.Logger;
import org.apache.tools.ant.types.Commandline;
import org.eclipse.jdt.annotation.NonNull;

import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.monitoring.runtime.instrumentation.common.com.google.common.base.Throwables;
import com.zillabyte.motherbrain.shell.MachineType;
import com.zillabyte.motherbrain.universe.Universe;

public final class Utils {

  private static final int SCHEDULE_POOL_SIZE = 16;
  
  private static Logger execLog = Utils.getLogger(Utils.class);
  private static ExecutorService _executorPool = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("util-pool-%s").build());
  private static ScheduledExecutorService _scheduleExecutorPool = Executors.newScheduledThreadPool(SCHEDULE_POOL_SIZE, new ThreadFactoryBuilder().setNameFormat("util-schedule-pool-%s").build());
  private static String _cachedUserName = null;
  
  
  public static ExecutorService createPrefixedExecutorPool(String prefix) {
    return Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat(prefix + "-%s").build());
  }
  

  
  
  
  
  /***
   * If dest only ever monotonically increases, this function is guaranteed to return with
   * dest having a value of at least tryValue and preserves monotonicity.
   * @param dest
   * @param tryValue
   * @return
   */
  public static final boolean compareAndSetIfGreater(final AtomicLong dest, final long tryValue) {
    long destValue;
    do {
      destValue = dest.get();
      if (tryValue <= destValue) {
        return false;
      }
    } while (!dest.compareAndSet(destValue, tryValue));
    return true;
  }

  
  
  /***
   * 
   * @param clazz
   * @return
   */
  public static final Logger getLogger(final @NonNull Class<?> clazz) {
    return Logger.getLogger(clazz);
  }
  
  

  /***
   * 
   * @return
   */
  public static String getSystemUserName() {
    if (_cachedUserName == null) {
      _cachedUserName = System.getProperty("user.name");
    }
    return _cachedUserName;
  }

  
  
  /***
   * 
   * @return
   */
  public static final MachineType getMachineType() {
    switch (getSystemUserName().toLowerCase()) {
    case "ubuntu":
      return MachineType.UBUNTU_EC2;
    case "vagrant":
      return MachineType.UBUNTU_VAGRANT;
    case "root":
      return MachineType.UBUNTU_TEAMCITY;
    default:
      return MachineType.OSX_LOCAL;
    }
  }

  
  /***
   * 
   * @param runnable
   * @return
   */
  public static Future<?> run(Runnable runnable) {
    return _executorPool.submit(runnable);
  }
  
  public static <T> Future<T> run(Callable<T> callable) {
    return _executorPool.submit(callable);
  }
  
  
  private static List<Future> _futures = Lists.newLinkedList();
  private static <T extends Future> T trackFuture(T future) {
    synchronized(_futures) {
      _futures.add(future);
      return future;
    }
  }
  
  public static void killAllFutures() {
    synchronized(_futures) {
      for(Future f : _futures) {
        if ( !f.isDone() && !f.isCancelled()) { 
          execLog.warn("killing future: " + f);
          f.cancel(true);
        }
      }
      _futures.clear();
      _scheduleExecutorPool.shutdown();
      _executorPool.shutdown();
      _executorPool = Executors.newCachedThreadPool();
      _scheduleExecutorPool = Executors.newScheduledThreadPool(SCHEDULE_POOL_SIZE);
    }
  }
  

  /***
   * 
   * @param delay
   * @param unit
   * @param command
   * @return
   */
  public static ScheduledFuture<?> schedule(long delay, TimeUnit unit, Runnable command) {
    return trackFuture(_scheduleExecutorPool.schedule(command, delay, unit));
  }
  
  public static ScheduledFuture<?> schedule(long delay, Runnable command) {
    return schedule(delay, TimeUnit.MILLISECONDS, command);
  }
  
  public static <T> ScheduledFuture<T> schedule(long delay, TimeUnit unit, Callable<T> command) {
    return trackFuture(_scheduleExecutorPool.schedule(command, delay, unit));
  }
  
  public static <T> ScheduledFuture<T> schedule(long delay, Callable<T> command) {
    return schedule(delay, TimeUnit.MILLISECONDS, command);
  }
  
  public static ScheduledFuture<?> scheduleDedicated(long delay, TimeUnit unit, Runnable command) {
    return trackFuture(Executors.newSingleThreadScheduledExecutor().schedule(command, delay, unit));
  }
  
  public static ScheduledFuture<?> scheduleDedicated(long delay, Runnable command) {
    return schedule(delay, TimeUnit.MILLISECONDS, command);
  }
  
  public static <T> ScheduledFuture<T> scheduleDedicated(long delay, TimeUnit unit, Callable<T> command) {
    return trackFuture(Executors.newSingleThreadScheduledExecutor().schedule(command, delay, unit));
  }
  
  public static <T> ScheduledFuture<T> scheduleDedicated(long delay, Callable<T> command) {
    return schedule(delay, TimeUnit.MILLISECONDS, command);
  }
  
  
  
  /***
   * 
   * @param ms
   * @param runnable
   * @throws InterruptedException
   * @throws ExecutionException
   * @throws TimeoutException
   */
  public static void executeWithin(long ms, Runnable runnable) throws InterruptedException, ExecutionException, TimeoutException {
    executeWithin(ms, TimeUnit.MILLISECONDS, runnable);
  }
  
  public static void executeWithin(long time, TimeUnit unit, Runnable runnable) throws InterruptedException, ExecutionException, TimeoutException {
    Future<?> future = Utils.run(runnable);
    future.get(time, unit);
  }
  
  
  public static <T> T executeWithin(long ms, Callable<T> runnable) throws InterruptedException, ExecutionException, TimeoutException {
    return executeWithin(ms, TimeUnit.MILLISECONDS, runnable);
  }
  
  public static <T> T executeWithin(long time, TimeUnit unit, Callable<T> runnable) throws InterruptedException, ExecutionException, TimeoutException {
    Future<T> future = Utils.run(runnable);
    return future.get(time, unit);
  }
  
  

  
  
  /***
   * 
   * @param initialDelay
   * @param delay
   * @param unit
   * @param command
   * @return
   */
  public static ScheduledFuture<?> timerFromPool(long initialDelay, long delay, TimeUnit unit, Runnable command) {
    return _scheduleExecutorPool.scheduleWithFixedDelay(command, initialDelay, delay, unit);
  }
  
  public static ScheduledFuture<?> timerFromPool(long initialDelay, long delay, Runnable command) {
    return timerFromPool(initialDelay, delay, TimeUnit.MILLISECONDS, command);
  }
  
  public static ScheduledFuture<?> timerFromPool(long delay, Runnable command) {
    return timerFromPool(0, delay, TimeUnit.MILLISECONDS, command);
  }
  
  public static ScheduledFuture<?> timerFromPool(long delay, TimeUnit unit, Runnable command) {
    return timerFromPool(0, delay, unit, command);
  }
  
  
  
  /****
   * 
   * @param initialDelay
   * @param delay
   * @param unit
   * @param command
   * @return
   */
  public static ScheduledFuture<?> timerDedicated(long initialDelay, long delay, TimeUnit unit, Runnable command) {
    return trackFuture(Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(command, initialDelay, delay, unit));
  }
  
  public static ScheduledFuture<?> timerDedicated(long initialDelay, long delay, Runnable command) {
    return timerDedicated(initialDelay, delay, TimeUnit.MILLISECONDS, command);
  }
  
  public static ScheduledFuture<?> timerDedicated(long delay, Runnable command) {
    return timerDedicated(0, delay, TimeUnit.MILLISECONDS, command);
  }
  
  public static ScheduledFuture<?> timerDedicated(long delay, TimeUnit unit, Runnable command) {
    return timerDedicated(0, delay, unit, command);
  }
  
  
  

  
  /**
   * 
   * @param path
   * @return
   */
  public static String expandPath(String path) {
    if (path.startsWith("~" + File.separator)) {
      return System.getProperty("user.home") + path.substring(1);
    }
    return path;
  }

  
  /***
   * 
   * @param e
   * @param klass
   * @return
   */
  public static <T extends Throwable> T getRootException(final Throwable e, final Class<T> klass) {
    // We can infinitely recurse examining getCause here, so we use a set to keep
    // track of what we've seen.  In theory someone devious could probably rewrite
    // getCause() to return a different Throwable each time AND recurse infinitely,
    // but hopefully that will be rare.
    Set<Throwable> seenSet = new HashSet<>();
    Throwable ex = e;
    T root = null;
    while (ex != null) {
      if (seenSet.contains(ex)) {
        // infinite loop, abort
        return null;
      }
      seenSet.add(ex);
      if (klass.isInstance(ex)) {
        root = klass.cast(ex);
      }
      ex = ex.getCause();
    }
    return root;
  }

  
  /***
   * 
   * @param e
   * @param klass
   * @return
   */
  public static <T extends Throwable> T getInitialException(final Throwable e, final Class<T> klass) {
    // We can infinitely recurse examining getCause here, so we use a set to keep
    // track of what we've seen.  In theory someone devious could probably rewrite
    // getCause() to return a different Throwable each time AND recurse infinitely,
    // but hopefully that will be rare.
    Set<Throwable> seenSet = new HashSet<>();
    Throwable ex = e;
    while (ex != null) {
      if (seenSet.contains(ex)) {
        // infinite loop, abort
        return null;
      }
      seenSet.add(ex);
      if (klass.isInstance(ex)) {
        return klass.cast(ex);
      }
      ex = ex.getCause();
    }
    return null;
  }

  
  
  /***
   * 
   * @param ex
   * @return
   * @throws InterruptedException
   */
  public static <T extends Throwable> T handleInterruptible(final T ex) throws InterruptedException {
    if (getInitialException(ex, ClosedByInterruptException.class) != null) {
      // This is how you detect InterruptedException from within BufferedReader.
      // I figured this out by looking at the source code for the GNU nio Channels
      // interface but it's backed up by
      // http://java.sun.com/j2se/1.5.0/docs/api/java/nio/channels/ClosedByInterruptException.html
      throw (InterruptedException) new InterruptedException().initCause(ex);
    }

    if (getInitialException(ex, InterruptedException.class) != null) {
      // There was an underlying InterruptedException that was chained below another error.
      throw (InterruptedException) new InterruptedException().initCause(ex);
    }
    
    // The exception probably wasn't an InterruptedException.
    return ex;
  }

  
  
  /***
   * 
   * @param command
   * @param stdout
   * @param stderr
   * @return
   * @throws InterruptedException
   * @throws IOException
   */
  public static int shell(Map<String,String> env, String[] command, final StringBuilder stdout, final StringBuilder stderr, MutableObject processCapture) throws InterruptedException, IOException {
    
    // Init
    ProcessBuilder pb = new ProcessBuilder(command);
    pb.environment().putAll(Universe.instance().shellFactory().getEnvironment());
    pb.environment().putAll(env);
    execLog.info("shell: " + Arrays.toString( command )  + " " + (env.size() > 0 ? "with env:" + env : ""));
    Process proc;
    try {
      proc = pb.start();
      if (processCapture != null) processCapture.setValue(proc);
    } catch (IOException e) {
      throw handleInterruptible(e);
    }
    
    final BufferedReader stderrBuffer = new BufferedReader(new InputStreamReader(proc.getErrorStream()));
    final BufferedReader stdoutBuffer = new BufferedReader(new InputStreamReader(proc.getInputStream()));

    Future<?> stderrThread = run(new Callable<Void>() {
      @Override
      public Void call() throws IOException {
        while (!Thread.currentThread().isInterrupted()) {
          final String line;
          try {
            line = stderrBuffer.readLine();
          } catch(IOException e) {
            try {
              throw handleInterruptible(e);
            } catch (InterruptedException e1) {
              return null;
            }
          }
          if (line == null) {
            return null;
          }
          execLog.info("STDERR: " + line);
          if (stderr != null) {
            stderr.append(line);
            stderr.append("\n");
          }
        }
        return null;
      }
    });

    Future<?> stdoutThread = run(new Callable<Void>() {
      @Override
      public Void call() throws IOException {
        while (!Thread.currentThread().isInterrupted()) {
          final String line;
          try {
            line = stdoutBuffer.readLine();
          } catch(IOException e) {
            try {
              throw handleInterruptible(e);
            } catch (InterruptedException e1) {
              return null;
            }
          }
          if (line == null) {
            return null;
          }
          execLog.info("stdout: " + line);
          if (stdout != null) {
            stdout.append(line);
            stdout.append("\n");
          }
        }
        return null;
      }
    });

    final int exitCode;
    try {
      exitCode = proc.waitFor();
    } finally {
      try {
        stdoutThread.get();
      } catch (ExecutionException e) {
        execLog.error(e, e.getCause());
      }
      try {
        stderrThread.get();
      } catch (ExecutionException e) {
        execLog.error(e, e.getCause());
      }
    }
    return exitCode;
  }
  
  public static int shell(String[] command, final StringBuilder stdout, final StringBuilder stderr, MutableObject processCapture) throws InterruptedException, IOException {
    return shell(Collections.EMPTY_MAP, command, stdout, stderr, processCapture);
  }
  public static int shell(String[] command, final StringBuilder stdout, final StringBuilder stderr) throws InterruptedException, IOException {
    return shell(command, stdout, stderr, null);
  }
  
  public static int shell(String command) throws InterruptedException, IOException {
    return shell(Commandline.translateCommandline(command), null, null);
  }
  
  public static int shell(String command, final StringBuilder stdout, final StringBuilder stderr, MutableObject processCapture) throws InterruptedException, IOException {
    return shell(Commandline.translateCommandline(command), stdout, stderr, processCapture);
  }
  
  public static int shell(String command, final StringBuilder stdout, final StringBuilder stderr) throws InterruptedException, IOException {
    return shell(Commandline.translateCommandline(command), stdout, stderr);
  }
  
  public static int shell(String command, final StringBuilder stdout) throws InterruptedException, IOException {
    return shell(Commandline.translateCommandline(command), stdout, null);
  }
  
  public static int shell(String... command) throws InterruptedException, IOException {
    return shell(command, null, null);
  }
  
  public static int shell(Map<String,String> env, String... command) throws InterruptedException, IOException {
    return shell(env, command, null, null, null);
  }
  
  public static int shell(Map<String, String> env, String command, StringBuilder stdout) throws InterruptedException, IOException {
    return shell(env, Commandline.translateCommandline(command), stdout, null, null);
  }
  
  public static int shell(Map<String, String> env, String command, StringBuilder stdout, StringBuilder stderr) throws InterruptedException, IOException {
    return shell(env, Commandline.translateCommandline(command), stdout, stderr, null);
  }




  /***
   * 
   * @param obj
   * @return
   */
  public static byte[] serialize(Object obj) {
    final byte[] objBytes;
    // Stolen from storm
    try {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(bos);
      oos.writeObject(obj);
      oos.close();
      objBytes = bos.toByteArray();
    } catch(IOException ioe) {
      throw new RuntimeException(ioe);
    }
    assert (objBytes != null);
    return objBytes;
  }
  
  
  /***
   * 
   * @param b
   * @return
   */
  @SuppressWarnings("unchecked")
  public static <T> T deserialize(byte[] b) {
    if (b == null) return null;
    final Object o;
    // Stolen from storm
    try {
      ByteArrayInputStream bis = new ByteArrayInputStream(b);
      ObjectInputStream ois = new ObjectInputStream(bis);
      Object ret = ois.readObject();
      ois.close();
      o = ret;
    } catch(IOException ioe) {
      throw new RuntimeException(ioe);
    } catch(ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    return (T)o;
  }
  

  /***
   * 
   * @param bw
   * @return
   */
  @SuppressWarnings("unchecked")
  public static <T> T deserialize(ByteArrayWrapper bw) {
    byte[] b = ByteArrayWrapper.unwrap(bw);
    return (T)deserialize(b);
  }

  
  
  /***
   * 
   * @param obj
   * @return
   */
  public static String serializeBase64(Object obj) {
    return Base64.encodeBase64String(serialize(obj));
  }
  
  /***
   * 
   * @param b
   * @return
   */
  public static Object deserializeBase64(String b) {
    return deserialize(Base64.decodeBase64(b));
  }

  
  /***
   * 
   * @param glob
   * @return
   */
  public static final String createRegexFromGlob(final String glob) {
    @RegEx
    final StringBuilder re = new StringBuilder(glob.length() * 2);
    
    re.append('^');
    for (final char c : glob.toCharArray()) {
      switch (c) {
      case '?':
        re.append('.');
        // fall through
      case '*':
        re.append('*');
        break;
      default:
        re.append(Pattern.quote(Character.toString(c)));
      }
    }
    re.append('$');
    
    return re.toString();
  }
  
  
  /**
   * Performs a wildcard matching for the text and pattern 
   * provided.
   *
   * Source: http://www.adarshr.com/papers/wildcard
   * 
   * @param text the text to be tested for matches.
   * 
   * @param pattern the pattern to be matched for.
   * This can contain the wildcard character '*' (asterisk).
   * 
   * @return <tt>true</tt> if a match is found, <tt>false</tt> 
   * otherwise.
   */
  public static boolean matchGlob(final StringBuilder text, final String pattern) {
    return Pattern.matches(createRegexFromGlob(pattern), text);
  }

  /***
   * 
   * @param text
   * @param pattern
   */
  public static boolean matchGlob(final String text, final String pattern) {
    return matchGlob(new StringBuilder(text), pattern);
  }


  /***
   * 
   * @return
   */
  public static String getHost() {
    try {
      Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
      while (interfaces.hasMoreElements()){
          NetworkInterface current = interfaces.nextElement();
          if (!current.isUp() || current.isLoopback() || current.isVirtual()) continue;
          if (current.getName().contains("lxc")) continue;  // <-- do not get LXC interfaces
          Enumeration<InetAddress> addresses = current.getInetAddresses();
          while (addresses.hasMoreElements()){
              InetAddress current_addr = addresses.nextElement();
              if (current_addr instanceof Inet4Address) {
                return current_addr.getHostAddress();
              }
          }
      }
    } catch(SocketException ex){ 
      return null;
    }
    return null;
  }


  
  /***
   * 
   * @param o
   * @param max
   * @return
   */
  public static String truncate(Object o, int max) {
    String s = o.toString();
    if (s.length() > max) {
      return s.substring(0, max) + "...";
    }
    return s;
  }
  
  
  /***
   * 
   * @param o
   * @return
   */
  public static String truncate(Object o) {
    return truncate(o, 30);
  }

  
  /***
   * 
   * @param ms
   */
  public static void sleep(long ms) {
    try {
      Thread.sleep(ms);
    } catch(InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  
  /***
   * 
   * @return
   */
  public static boolean isInterrupted() {
    return Thread.currentThread().isInterrupted();
  }



  /***
   * 
   * @param v
   * @return
   */
  @Deprecated
  public static <T> T valueOf(T v) {
    return v;
  }



  /***
   * 
   * @param col
   * @param key
   * @param val
   * @return
   */
  public static <K,V> V putIfAbsent(ConcurrentHashMap<K, V> col, K key, V val) {
    if (col.containsKey(key) == false) {
      col.put(key, val);
    }
    return col.get(key);
  }

  public static class CopyFileVisitor extends SimpleFileVisitor<Path> {
    private final Path targetPath;
    private Path sourcePath = null;
    private File lxcRootDir = null;
    
    public CopyFileVisitor(Path targetPath) {
      this.targetPath = targetPath;
    }
    
    public CopyFileVisitor(Path targetPath, File lxcPrefix) {
      this.targetPath = targetPath;
      this.lxcRootDir = lxcPrefix;
    }

    @Override
    public FileVisitResult preVisitDirectory(final Path dir, final BasicFileAttributes attrs) throws IOException {
      if (sourcePath == null) {
        sourcePath = dir;
      } else {
        Files.createDirectories(targetPath.resolve(sourcePath.relativize(dir)));
      }
      return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
      /***
       * Copying from and LXC container is a bit weird, especially when there are symlinks involved. Since we
       * run zillabyte prep within the container, all symlinks with absolute paths are "absolute" relative to
       * the LXC container's root directory (rootfs). Hence, when we copy them, we need to tack on the lxcRootDir
       * path. Relative symlinks are naturally resolved by java.
       */
      if(lxcRootDir != null && Files.isSymbolicLink(file)) {
        Path linkPath = Files.readSymbolicLink(file);
        if(linkPath.isAbsolute()) {
          linkPath = (new File(lxcRootDir.toString(), linkPath.toString())).toPath();
          Files.copy(linkPath, targetPath.resolve(sourcePath.relativize(file)));
          return FileVisitResult.CONTINUE;
        }
      }
      // Standard copy, resolves relative symlinks too
      Files.copy(file, targetPath.resolve(sourcePath.relativize(file)));
      return FileVisitResult.CONTINUE;
    }
  }


  
  /***
   * 
   */
  public static <T> T TODO() {
    throw new NotImplementedException();
  }
  public static <T> T TODO(String s) {
    throw new NotImplementedException(s);
  }


 /**
  * Attempt to make a call, retrying on timeout
  * @param times
  * @param callable
  * @return
  * @throws ExecutionException
  * @throws RetryException
  */
  public static <T> T retry(int times, Callable<T> callable) throws ExecutionException, RetryException {
    
    // Build the retryer
    RetryerBuilder<T> builder = RetryerBuilder.newBuilder();
    builder.withStopStrategy(StopStrategies.stopAfterAttempt(times));
    builder.withWaitStrategy(WaitStrategies.randomWait(2, TimeUnit.SECONDS, 20, TimeUnit.SECONDS));
    Retryer<T> retryer = builder.build();
    return retryer.call(callable);
    
  }
  
  public static <T> T retry(Callable<T> callable) throws ExecutionException, RetryException {
    return retry(3, callable);
  }
  
  
  /**
   * Retry a call multiple times, supressing non runtime exceptions
   * @param times
   * @param callable
   * @return
   */
  public static <T> T retryUnchecked(int times, Callable<T> callable) {
    try {
      return retry(times, callable);
    } catch (ExecutionException | RetryException e) {
      throw new RuntimeException(e);
    }
  }
  public static <T> T retryUnchecked(Callable<T> callable) {
    return retryUnchecked(3, callable);
  }



  public static boolean exists(String path) {
    path = Utils.expandPath(path);
    return new File(path).exists();
  }



  public static void bash(String command) throws InterruptedException, IOException {
    Utils.shell("/bin/bash", "-l", "-c", command);
  }

  
  public static boolean isCause(Throwable error, Class<? extends Exception> klass) {
    return ExceptionUtils.indexOfType(error, klass) != -1;
  }



  public static String stripLeading(String s, String leading) {
    if (s.startsWith(leading)) {
      return s.replaceFirst(Pattern.quote(leading), "");
    } else {
      return s;
    }
  }



  public static String prefixKey(String key) {
    String prefix = Universe.instance().env().name();
    String f = prefix + key.replaceFirst("$/+", "");
    return f.replaceFirst("$/+", ""); // don't start keys with slashes.
  }



  public static <T> T propagate(Exception e) {
    Throwables.propagate(e);
    return null;
  }


   
}
