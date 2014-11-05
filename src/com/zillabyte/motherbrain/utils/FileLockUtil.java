package com.zillabyte.motherbrain.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.FileLock;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;

import com.google.common.collect.Maps;

public class FileLockUtil {

  static final ConcurrentMap<String, Semaphore> _jvmLocks = Maps.newConcurrentMap();
  
  
  public static class MultiLock implements AutoCloseable {
    
    FileLock _fileLock;
    FileOutputStream _stream;
    Semaphore _jvmLock;
    
    MultiLock() {}
    
    public void release() throws IOException, InterruptedException {
      try {
        close();
      } catch (IOException e) {
        throw Utils.handleInterruptible(e);
      }
    }

    @Override
    public void close() throws IOException {
      InterruptedException interrupt = null;
      while(true) {
        try {
          _fileLock.release();
          break;
        } catch (IOException e) {
          try {
            throw Utils.handleInterruptible(e);
          } catch (InterruptedException e1) {
            // Should not throw InterruptedException in AutoCloseable
            if (interrupt == null) {
              interrupt = e1;
            } else {
              interrupt.addSuppressed(e1);
            }
          }
        }
      }
      try {
        while(true) {
          try {
            _stream.close();
            break;
          } catch (IOException e) {
            try {
              throw Utils.handleInterruptible(e);
            } catch (InterruptedException e1) {
              // Should not throw InterruptedException in AutoCloseable
              if (interrupt == null) {
                interrupt = e1;
              } else {
                interrupt.addSuppressed(e1);
              }
            }
          }
        }
      } finally {
        // Even if we can't close (possibly leak) the stream, it should be
        // safe to release the JVM lock.
        _jvmLock.release();
      }
      if (interrupt != null) {
        Thread.currentThread().interrupt();
        throw (ClosedByInterruptException) new ClosedByInterruptException().initCause(interrupt);
      }
    }
  }

  public static MultiLock lock(String lockFile) throws InterruptedException, IOException {
    return lock(new File(lockFile));
  }
  
  public static MultiLock lock(File lockFile) throws InterruptedException, IOException {
    
    // Init the jvm lock 
    _jvmLocks.putIfAbsent(lockFile.getAbsolutePath(), new Semaphore(1));
    final Semaphore jvmLock = _jvmLocks.get(lockFile.getAbsolutePath());
    
    // Acquire the JVM lock.
    jvmLock.acquire();
    
    // Success, we have a jvm lock.  Now can we get a file lock? 
    lockFile.getParentFile().mkdirs();
    
    // Create the file..
    if (lockFile.exists() == false) {
      try {
        lockFile.createNewFile();
        lockFile.deleteOnExit();
      } catch (IOException e) {
        // Error creating the file, release the lock and rethrow.
        jvmLock.release();
        throw Utils.handleInterruptible(e);
      }
    }
    final MultiLock mlock = new MultiLock();
    try {
      mlock._stream = new FileOutputStream(lockFile);
    } catch (FileNotFoundException e) {
      // We could try creating the file again, but perhaps better to fail
      // here to avoid potential DOS.  Instead, release the lock and rethrow.
      jvmLock.release();
      throw e;
    }
    try {
      try {
        mlock._fileLock = mlock._stream.getChannel().lock();
      } catch(java.nio.channels.FileLockInterruptionException e) {
        throw (InterruptedException) new InterruptedException().initCause(e);
      }
    } catch(java.nio.channels.OverlappingFileLockException e) {
      // The JVM has a lock that we don't know about.  Clean up
      // and rethrow.
      InterruptedException interrupt = null;
      try {
        while (true) {
          try {
            // Try to close the stream.
            mlock._stream.close();
            break;
          } catch (IOException e1) {
            try {
              throw Utils.handleInterruptible(e1);
            } catch (InterruptedException e2) {
              // If we were interrupted, retry the close.
              interrupt = e2;
            }
          }
        }
      } finally {
        // Release the lock.
        jvmLock.release();
        if (interrupt != null) {
          throw interrupt;
        }
      }
      // Rethrow
      throw e;
    } catch (IOException e) {
      InterruptedException interrupt = null;
      try {
        while (true) {
          try {
            // Try to close the stream.
            mlock._stream.close();
            break;
          } catch (IOException e1) {
            try {
              throw Utils.handleInterruptible(e1);
            } catch (InterruptedException e2) {
              // If we were interrupted, retry the close.
              interrupt = e2;
            } catch (IOException e2) {
              mlock._fileLock.close();
            }
          }
        }
      } finally {
        // Release the lock.
        jvmLock.release();
        if (interrupt != null) {
          throw interrupt;
        }
      }
      // Rethrow.
      throw Utils.handleInterruptible(e);
    }
    // Success!
    mlock._jvmLock = jvmLock;
    return mlock;
  }
}
