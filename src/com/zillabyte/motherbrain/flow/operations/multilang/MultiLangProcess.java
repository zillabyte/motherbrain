package com.zillabyte.motherbrain.flow.operations.multilang;

//import com.zillabyte.motherbrain.utils.CLibrary;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import net.sf.json.JSONObject;

import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;
//import com.sun.jna.LastErrorException;
import com.zillabyte.motherbrain.benchmarking.Benchmark;
import com.zillabyte.motherbrain.flow.operations.LoopException;
import com.zillabyte.motherbrain.flow.operations.OperationLogger;
import com.zillabyte.motherbrain.universe.Config;
import com.zillabyte.motherbrain.universe.Universe;
import com.zillabyte.motherbrain.utils.JSONUtil;
import com.zillabyte.motherbrain.utils.Utils;


/****
 * MultiLangProcess should be considered the lowest-level abstraction on top of
 * the multilangs.  It's only purpose is to queue up input & output messages, and 
 * to pass basic logging messages back to the caller. 
 * 
 * Instead of adding different use-case logic to this class, consider creating a 
 * new MultiLangObserver, which is essentially a wrapper around this and makes
 * it easier to reason about the underlying mechanics
 * 
 * @author jake
 */
public class MultiLangProcess {

  
  public static enum State {
    INITIAL,
    RUNNING,
    PAUSED,
    DEAD
  }

  static Logger _log = Logger.getLogger(MultiLangProcess.class);
  public final int SUSPEND_LIMIT = Config.getOrDefault("multilang.suspend.limit", Utils.valueOf(2500)).intValue();  // The number of tuples to collect before going into PAUSE 
  public static final long SUSPEND_WAIT_TIME_MS = TimeUnit.MILLISECONDS.convert(10, TimeUnit.SECONDS);
  public static final long HANDSHAKE_TIMEOUT_MS = TimeUnit.MILLISECONDS.convert(25, TimeUnit.SECONDS);
  public static final long PROCESS_KILL_TIMEOUT_SECONDS = TimeUnit.SECONDS.convert(10, TimeUnit.SECONDS);
  static final String EOF_SIGNAL = new String(); 
  private static final long MAX_WAIT_FOR_MESSAGE_TIMEOUT_SECONDS = TimeUnit.SECONDS.convert(1, TimeUnit.HOURS);
  private static final long SOCKET_CONNECT_TIMEOUT_MS = 1000L * 30;
  
  private InputStream _stdout;
  private InputStream _stderr;
  private Future<?> _outputWatcher = null;
  BufferedReader _inputReader;
  LinkedBlockingQueue<String> _messagesToProcess;
  LinkedBlockingQueue<String> _messagesFromProcess; 
  Process _process;
  Writer _outputWriter;
  private Future<?> _inputWatcher = null;
  BufferedReader _stdoutBuffer;
  BufferedReader _stderrBuffer;
  private Future<?> _stderrThread;
  private Future<?> _stdoutThread;
  String _pid = null;
  private Future<?> _emitThread;
  List<MultiLangMessageHandler> _messageListeners = Lists.newCopyOnWriteArrayList();
  List<MultiLangLogHandler> _logListeners = Lists.newCopyOnWriteArrayList();
  List<MultiLangErrorHandler> _errorListeners = Lists.newCopyOnWriteArrayList();
  private Future<?> _messageListenerThread = null;
  final AtomicReference<State> _state = new AtomicReference<>(State.INITIAL);
  Object _stateSemaphore = new Object();
  private ProcessBuilder _processBuilder;
  private Future<Integer> _processWrapper;
  private ServerSocket _serverSocket = null;
  private Socket _socket = null;
  private Benchmark _benchmark = Universe.instance().benchmarkFactory().create(); 
  
  /***
   * 
   * @param pb
   * @param inputPipe
   * @param outputPipe
   * @throws MultiLangException 
   * @throws InterruptedException 
   * @throws IOException 
   * @throws MultiLangProcessException 
   */
  public MultiLangProcess(final ProcessBuilder pb, ServerSocket socket) {
    _processBuilder = pb;
    _serverSocket = socket;
    _messagesToProcess = new LinkedBlockingQueue<>();
    _messagesFromProcess = new LinkedBlockingQueue<>();
  }
  
  
  /***
   * 
   * @return
   * @throws MultiLangProcessException
   */
  public MultiLangProcess start() {
    try {
      
      // Start the actual process..
      _benchmark.begin("multilang.process.start");
      handleStartProcess(_processBuilder);
      ensureAlive();
  
      // watched named inputs...
      if (_serverSocket != null) {
        _inputWatcher = createInputMessageThread();
        _outputWatcher = createOutputMessageThread();
      }

    } finally {
      _benchmark.end("multilang.process.start");
    }
    return this;
  }



  /****
   * 
   * @param pb
   * @throws IOException 
   * @throws MultiLangException 
   */
  private void handleStartProcess(final ProcessBuilder pb) {
    
    // Init 
    _log.info("starting the process: " + pb.command() + " in " + pb.directory());
    final MutableBoolean socketRunning = new MutableBoolean(false);
    if (_state.compareAndSet(State.INITIAL, State.RUNNING)) {
      
      // Start the server...
      Future<Socket> socketFuture = null;
      if (this._serverSocket != null) {
        socketFuture = Utils.run(new Callable<Socket>() {
          public Socket call() {
            _benchmark.begin("multilang.process.socket.init");
            try {
              _serverSocket.setPerformancePreferences(0, 2, 1);
              socketRunning.setValue(true);
              Socket socket;
              try {
                socket = _serverSocket.accept();
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
              return socket;
            } finally {
              _benchmark.end("multilang.process.socket.init");
            }
          }
        });
        
        // Make sure the socket listener starts up... 
        while(socketRunning.isFalse()) {
          // Note: we purposely don't use .wait here because we don't want the above
          // chunk of code to context switch. 
          Utils.sleep(10);
        }
      }
      
      // Run
      _benchmark.begin("multilang.process.start.actual");
      try {
        _process = pb.start();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      // observe stdin and stdout
      _stdout = _process.getInputStream();
      _stderr = _process.getErrorStream();
      _stdoutBuffer = new BufferedReader(new InputStreamReader(_stdout));
      _stderrBuffer = new BufferedReader(new InputStreamReader(_stderr));
      _stderrThread = createStderrThread();
      _stdoutThread = createStdoutThread();
      
      _benchmark.end("multilang.process.start.actual");
      
      // Wrap it around a thread so we know exactly when it finishes.
      _processWrapper = Utils.run(new Callable<Integer>() {
        
        @Override
        public Integer call() {
          
          // Wait until the process is done
          try {
            _process.waitFor();
          } catch (InterruptedException e) {
            _log.info("process '" + pb.command() + "' exited with by interruption");
            return Integer.valueOf(0);
          }
          
          // Done 
          _log.info("process '" + pb.command() + "' exited with value: " + _process.exitValue());
          if (_state.getAndSet(State.DEAD) != State.DEAD) {        
            // Tell listeners to stop
            if (_messagesFromProcess != null) {
              _messagesFromProcess.add(EOF_SIGNAL);
            }
            if (_messagesToProcess != null) {
              _messagesToProcess.add(EOF_SIGNAL);
            }
          }
          return Integer.valueOf(_process.exitValue());
        }
      });

      // Wait for a connection to the socket...
      if (this._serverSocket != null) {
        try {
          _socket = socketFuture.get(SOCKET_CONNECT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
          String extraInfo = "";
          if (_processWrapper.isDone()) {
            extraInfo = " The process is done;";
          } else {
            extraInfo = " The process is NOT done;";
          }
          cleanup();
          throw new RuntimeException("Could not connect with multilang socket: " + _serverSocket.getLocalPort() + ". " + extraInfo + ".");
        } catch (ExecutionException e) {
          cleanup();
          throw new RuntimeException(e);
        } catch (InterruptedException e) {
          // Do nothing...
        }
      }
      
    }
  }


  
  /***
   * Helper to make sure process is running
   * @throws InterruptedException 
   */
  void ensureAlive() {
    if (isAlive() == false) {
      
      // We're dead.  If the process has any errors, rethrow them here..
      assert(this._processWrapper.isDone());
      try {
        this._processWrapper.get();
      } catch (ExecutionException | InterruptedException e) {
        throw new RuntimeException(e);
      }
      
      // Otherwise, no errors.. just throw the dead exception...
      throw new RuntimeException("The multilang process died!");
        
    }
  }
  
  

  public void handleHandshake() {

    // First, we tell it where the pid dir is...
    Utils.sleep(1000);
    ensureAlive();
    writeMessage("{\"pidDir\": \"/tmp\"}");
    writeMessage("end");
    ensureAlive();
    
    // Then it tells us it's pid (and other potential stuff)
    String firstLine = getNextMessage(60000, TimeUnit.MILLISECONDS);
    String secondLine = getNextMessage(60000, TimeUnit.MILLISECONDS);
    
    if (firstLine == null || secondLine == null) {
      throw new RuntimeException("Handshake timeout!: " + firstLine);
    }
    
    if (secondLine.equalsIgnoreCase("end")) {
      JSONObject obj = (JSONObject) JSONUtil.parseObj(firstLine);
      this._pid = obj.getString("pid");
    } else {
      throw new RuntimeException("Invalid handshake, bad json: " + firstLine);
    }
   
  }

  


  /***
   * 
   * @param outputPipe
   */
  private Future<?> createOutputMessageThread() {
    return Utils.run(new Callable<Void>() {

      @Override
      public Void call() throws IOException {
        
        _outputWriter = new OutputStreamWriter(_socket.getOutputStream());
        
        //_log.info("writer created.");
        try {
          while (isAlive() && !Thread.currentThread().isInterrupted()) {
            // Get the next message. 
            final String line;
            try {
              line = _messagesToProcess.take();
            } catch (InterruptedException e) {
              return null;
            }
            if (line == EOF_SIGNAL) {
              // quit thread
              return null;
            }
            if (line != null) {
              debug("sending message: " + line);
              _outputWriter.write(line + "\n");
              _outputWriter.flush();
            }
          }
        } finally {
          //_log.info("writer dead");
          _outputWriter.close();
        }
        return null;
      }
    });
  }

  
  /***
   * 
   */
  public boolean isAlive() {
    return this._state.get() != State.DEAD;
  }

  
  /****
   * 
   */
  public Process getProcess() {
    return _process;
  }
  
  
  /***
   * 
   */
  public ProcessBuilder getProcessBuilder() {
    return _processBuilder;
  }

  
  /***
   * 
   * @param inputPipe
   */
  private Future<?> createInputMessageThread() {
    return Utils.run(new Callable<Void>() {

      @Override
      public Void call() throws IOException {
        
        _inputReader = new BufferedReader(new InputStreamReader(_socket.getInputStream()));
        
        // _log.info("reader created.");
        try {
          while (isAlive() && !Thread.currentThread().isInterrupted()) {
            // Get the next message. 
            final String line;
            
            //Benchmark.markBegin("multilang.process.input_reader");
            try {
              line = _inputReader.readLine();
            } catch(IOException e) {
              throw new RuntimeException(e);
            } finally {
              //Benchmark.markEnd("multilang.process.input_reader");
            }
            
            if (line != null) {
              debug("received line: " + line);
              _messagesFromProcess.add(line);
            }
            
            // Are we being overrun??? If so, pause the process until we catch up.. 
            if (_messagesFromProcess.size() > SUSPEND_LIMIT) {
              if (_pid != null) {
                pause();
                try {
                  // Spin wait until the buffer subsides
                  do {
                    ensureAlive();
                    _log.warn("process suspended until message queue is consumed: " + _messagesFromProcess.size());
                    for(MultiLangLogHandler l : _logListeners) {
                      l.onSystemError("process suspended while downstream operations catch up", _log);
                    }
                    Thread.sleep(SUSPEND_WAIT_TIME_MS);
                  } while(_messagesFromProcess.size() > 0);
                } catch (InterruptedException e) {
                  /*
                   * Swallow, thread boundary.
                   */
                  return null;
                } finally {
                  resume();
                }
              } else {
                _log.warn("process is being overrun and we can't suspend the child process!");
              }
            }
          }
        } finally {
          //_log.info("reader dead");
          _inputReader.close();
        }
        return null;
      }
    });
  }



  /***
   * 
   */
  private Future<?> createStdoutThread() {
    return Utils.run(new Callable<Void>() {
      @Override
      public Void call() {
        
        String line;
        try {
          while( (line=_stdoutBuffer.readLine()) != null ) {
            
            // Process the line... 
            for(MultiLangLogHandler l : _logListeners) {
              l.onStdOut(line, _log);
            }
          }
        } catch(IOException e) {
          _log.warn("IOException in Stdout reader, probably just a dead process: " + e);
        }
        return null;
      }
    });
  }




  /***
   * 
   */
  private Future<?> createStderrThread() {
    return Utils.run(new Callable<Void>() {
      @Override
      public Void call() throws IOException {
        
        String line;
        try {
          while( (line=_stderrBuffer.readLine()) != null ) {
            
            // Process the line... 
            for(MultiLangLogHandler l : _logListeners) {
              l.onStdErr(line, _log);
            }
          }
        } catch(IOException e) {
          _log.warn("IOException in Stderr reader, probably just a dead process: " + e);
        }
        return null;
      }
    });
  }
  
    
  
  /***
   * 
   * @param wait
   * @param unit
   * @throws MultiLangProcessException 
   * @throws InterruptedException
   */
  public String getNextMessage(long wait, TimeUnit unit) {
    try {
      
      // INIT 
      ensureAlive();
      String m = _messagesFromProcess.poll(wait, unit);
      
      if (m == EOF_SIGNAL) {
        try {
          _processWrapper.get();
        } catch (ExecutionException e) {
          throw new RuntimeException("An error occurred reading from the multilang process.");
        }
        return null;
      }
      
      // DONE 
      return m;
    } catch(InterruptedException e) {
      throw new RuntimeException("An error occurred reading from the multilang process.");
    }
  }
  
  
  /***
   * 
   * @param wait
   * @return
   * @throws MultiLangProcessException
   */
  public String getNextMessage(long wait) {
    return getNextMessage(wait, TimeUnit.MILLISECONDS);
  }
  
  
  /***
   * 
   * @throws InterruptedException 
   * @throws MultiLangProcessException 
   */
  public String getNextMessage() {
    return getNextMessage(MAX_WAIT_FOR_MESSAGE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
  }
  
  

  
  
  
  /***
   * 
   * @param line
   * @throws MultiLangProcessException 
   * @throws InterruptedException 
   * @throws MultiLangProcessDeadException 
   */
  public void writeMessage(String line) {
    ensureAlive();
    _messagesToProcess.add(line);
  }


  
  
  
  
  /***
   * 
   * @throws MultiLangException
   */
  public static String getKillCommand(String pidString) {
    int pid = Integer.parseInt(pidString);
    return "kill " + pid;
  }
  
  
  public String getPid() {
    return this._pid;
  }
  
  
  public void cleanup()  {
    
    _log.info("beginning cleanup");
    
    //_log.info("deleting pipes...");
    try {
      if (_serverSocket != null) {
        _serverSocket.close();
        if (_socket != null) {
          _socket.close();
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    if (_stderrThread != null) {
      _stderrThread.cancel(true);
    }
    if (_stdoutThread != null) {
      _stdoutThread.cancel(true);
    }
    if (_emitThread != null) {
      _emitThread.cancel(true);
    }
    if (_messageListenerThread != null) {
      _messageListenerThread.cancel(true);
    }
    if (_outputWatcher != null) {
      _outputWatcher.cancel(true);
    }
    if (_inputWatcher != null) {
      _inputWatcher.cancel(true);
    }
    
    //_log.info("Cleaned Up");
    
  }
  
  
  /**
   * @throws InterruptedException 
   * @throws MultiLangException  
   * @throws MultiLangProcessException 
   * 
   */
  public void destroy() {
    if (_state.get() == State.DEAD) {
      // Already dead, or already in the process of killing?
      cleanup();
      return;
    }

    // Kill!
    _log.info("Beginning destroy " + this._pid + " (" + this._processBuilder.command() + ")");
    
    if (_pid != null) {
      Process p;
      try {
        final String killCommand = getKillCommand(_pid);
        _log.error(killCommand);
        p = Runtime.getRuntime().exec(killCommand);
        p.waitFor();
      } catch (IOException | InterruptedException e) {
        throw new RuntimeException("Failed to kill the multilang process.");
      }
      
      _log.info("Process " + _pid + " killed with exit status: " + p.exitValue());
    }

    // Will the `kill` command do it? 
    try {
      this.waitForExit(PROCESS_KILL_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } catch (TimeoutException | InterruptedException e) {
      // Timeout.. force kill this dude
      _log.info("Process did not die after 'kill' command. Forcing kill");
      this._process.destroy();
    }
    
    _log.info("Killed.");
  }

  
  

  /***
   * Pauses the multilang
   * @throws InterruptedException 
   * @throws MultiLangException 
   */
  public void pause() {
    if (_state.compareAndSet(State.RUNNING, State.PAUSED)) {
      _log.info("sending kill -SIGSTOP to " + _pid);
      Utils.shell("kill -SIGSTOP " + _pid);
    } else {
      throw new RuntimeException("Attempted to pause a multilang process (PID=" + _pid + ") that was not running!");
    }
  }


  /***
   * Resumes the multilang
   * @throws InterruptedException 
   * @throws MultiLangException 
   */
  public void resume() {
    if (_state.compareAndSet(State.PAUSED, State.RUNNING)) {
      _log.info("sending kill -SIGCONT to " + _pid);     
      Utils.shell("kill -SIGCONT " + _pid);
    } else {
      throw new RuntimeException("Attempted to resume a multilang process (PID=" + _pid + ") that was not paused!");
    }
  }
  
  
  public boolean isPaused() {
    return _state.get() == State.PAUSED;
  }
  
  
  public MultiLangProcess waitForExit() throws InterruptedException {
    try {
      _process.waitFor();
    } finally {
      cleanup();
    }
    return this;
  }
  
  

  
  public void waitForExit(final long timeout, final TimeUnit unit) throws InterruptedException, TimeoutException {
    
    // Start running it...
    Future<?> f = Utils.run(new Callable<Void>() {
      @Override
      public Void call() {
        try {
          waitForExit();

        } catch (InterruptedException e) {
          /*
           * Thread boundary, swallow.
           */
        }
        return null;
      }
    });
    
    try {
      f.get(timeout, unit);
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }
  
  public void waitForExit(final long timeout) throws InterruptedException, TimeoutException {
    waitForExit(timeout, TimeUnit.MILLISECONDS);
  }

  public void addErrorListener(MultiLangErrorHandler handler) {
    this._errorListeners.add(handler);
  }

  public void removeErrorListener(MultiLangErrorHandler handler) {
    this._errorListeners.remove(handler);
  }
  
  


  /***
   * 
   * @param handler
   */
  public void addMessageListener(MultiLangMessageHandler handler) {
    
    // Add the handler
    this._messageListeners.add(handler);
    
    // Create the listener thread, if not already exists
    if (_messageListenerThread == null) {
      this._messageListenerThread = Utils.run(new Callable<Void>() {
  
        @Override
        public Void call() {
          while(!Utils.isInterrupted()) {
            final String nextLine;
            
            Benchmark.markBegin("multilang.process.message_listener");
            try {
              nextLine = getNextMessage();
            } finally {
              Benchmark.markEnd("multilang.process.message_listener");
            }
            
            for(MultiLangMessageHandler l : _messageListeners) {
              try {
                l.handleMessage(nextLine);
              } catch (LoopException e) {
                if (_errorListeners.size() == 0) {
                  System.err.println("no error listeners!");
                  e.printStackTrace();
                }
                for(MultiLangErrorHandler h : _errorListeners) {
                  h.handleError(e);
                }
              }
            }
          }
          return null;
        }
      });
    }
    
  }

  
  public void removeMessageListener(MultiLangMessageHandler handler) {
    this._messageListeners.remove(handler);
  }
  


  public void addLogListener(MultiLangLogHandler handler) {
    this._logListeners.add(handler);
  }

  
  public void removeLogListener(MultiLangLogHandler handler) {
    this._logListeners.remove(handler);
  }




  public void writeMessageWithEnd(String string) {
    writeMessage(string);
    writeMessage("end");
  }



  public List<MultiLangErrorHandler> getErrorListeners() {
    return this._errorListeners;
  }

  

  public MultiLangProcess addStdoutListener(final StringBuilder sb) {
    this.addLogListener(new MultiLangLogHandler() {

      @Override
      public void onStdErr(String s, Logger fallbackLogger) {
        sb.append(s);
        sb.append("\n");
      }

      @Override
      public void onStdOut(String s, Logger fallbackLogger) {
        sb.append(s);
        sb.append("\n");
      }

      @Override
      public void onSystemError(String s, Logger fallbackLogger) {
        sb.append(s);
        sb.append("\n");
      }

      @Override
      public void onSystemInfo(String s, Logger fallbackLogger) {
        sb.append(s);
        sb.append("\n");
      }
      
    });
    return this;
  }
  

  public MultiLangProcess addStdioLogListeners() {
    this.addLogListener(new MultiLangLogHandler() {

      @Override
      public void onStdErr(String s, Logger fallbackLogger) {
        System.err.println("stderr: " + s);;
      }

      @Override
      public void onStdOut(String s, Logger fallbackLogger) {
        System.err.println("stdout: " + s);;
      }

      @Override
      public void onSystemError(String s, Logger fallbackLogger) {
        System.err.println("stderr: " + s);;
      }

      @Override
      public void onSystemInfo(String s, Logger fallbackLogger) {
        System.err.println("stdout: " + s);;
      }

    });
    return this;
  }
  

  public MultiLangProcess addLogListener(final OperationLogger _logger) {
    this.addLogListener(new MultiLangLogHandler() {

      @Override
      public void onStdErr(String s, Logger fallbackLogger) {
        _logger.error(s);
      }

      @Override
      public void onStdOut(String s, Logger fallbackLogger) {
        _logger.info(s);
      }

      @Override
      public void onSystemError(String s, Logger fallbackLogger) {
        _logger.error(s);
      }

      @Override
      public void onSystemInfo(String s, Logger fallbackLogger) {
        _logger.info(s);
      }

    });
    return this;
  }




  /**
   * @param env 
   * @return 
   * @throws IOException 
   * @throws InterruptedException 
   * @throws MultiLangProcessException **
   * 
   */
  public static MultiLangProcess create(Map<String, String> env, String[] command, ServerSocket socket) {

    // Create named pipes...
    _log.info("executing: " + Arrays.toString(command) + " with socket: " + (socket == null ? "null" : socket.getLocalPort()));

    // Init: create the process env,
    ProcessBuilder pb = new ProcessBuilder(command);
    pb.environment().putAll(env);

    // Create multilang process
    MultiLangProcess mlp = new MultiLangProcess(pb, socket);
    return mlp;
    
  }


  
  public MultiLangProcess setWorkingDir(File dir) {
    _processBuilder = _processBuilder.directory(dir);
    return this;
  }

  
  /***
   * 
   */
  public MultiLangProcess debug(String s) {
    // System.err.println(s);
    return this;
  }


  
  
}
