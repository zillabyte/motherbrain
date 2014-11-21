package com.zillabyte.motherbrain.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
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
    
    public void release() {
      close();
    }

    @Override
    public void close() {
      
      while(true) {
        try {
          _fileLock.release();
          break;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
      try {
        while(true) {
          try {
            _stream.close();
            break;
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      } finally {
        // Even if we can't close (possibly leak) the stream, it should be
        // safe to release the JVM lock.
        _jvmLock.release();
      }

    }
  }

  public static MultiLock lock(String lockFile) {
    return lock(new File(lockFile));
  }
  
  public static MultiLock lock(File lockFile) {
    
    // Init the jvm lock 
    _jvmLocks.putIfAbsent(lockFile.getAbsolutePath(), new Semaphore(1));
    final Semaphore jvmLock = _jvmLocks.get(lockFile.getAbsolutePath());
    
    // Acquire the JVM lock.
    try {
      jvmLock.acquire();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    
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
        throw new RuntimeException(e);
      }
    }
    final MultiLock mlock = new MultiLock();
    try {
      mlock._stream = new FileOutputStream(lockFile);
    } catch (FileNotFoundException e) {
      // We could try creating the file again, but perhaps better to fail
      // here to avoid potential DOS.  Instead, release the lock and rethrow.
      jvmLock.release();
      throw new RuntimeException(e);
    }
    try {
      try {
        mlock._fileLock = mlock._stream.getChannel().lock();
      } catch(java.nio.channels.FileLockInterruptionException e) {
        throw new RuntimeException(e);
      }
    } catch(java.nio.channels.OverlappingFileLockException e) {
      // The JVM has a lock that we don't know about.  Clean up
      // and rethrow.
      try {
        while (true) {
          try {
            // Try to close the stream.
            mlock._stream.close();
            break;
          } catch (IOException e1) {
            throw new RuntimeException(e1);
          }
        }
      } finally {
        // Release the lock.
        jvmLock.release();
      }
      // Rethrow
      throw e;
    } catch (IOException e) {
      try {
        while (true) {
          try {
            // Try to close the stream.
            mlock._stream.close();
            break;
          } catch (IOException e1) {
            try {
              mlock._fileLock.close();
            } catch (IOException e2) {
              throw new RuntimeException(e2);
            }
            throw new RuntimeException(e1);
          }
        }
      } finally {
        // Release the lock.
        jvmLock.release();
      }

    }
    // Success!
    mlock._jvmLock = jvmLock;
    return mlock;
  }
}
