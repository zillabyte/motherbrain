package grandmotherbrain.flow.operations.multilang;

import grandmotherbrain.benchmarking.Benchmark;
import grandmotherbrain.flow.MapTuple;
import grandmotherbrain.flow.MetaTuple;
import grandmotherbrain.flow.collectors.OutputCollector;
import grandmotherbrain.flow.operations.Operation;
import grandmotherbrain.flow.operations.OperationException;
import grandmotherbrain.relational.DefaultStreamException;
import grandmotherbrain.universe.Config;
import grandmotherbrain.utils.JSONUtil;
import grandmotherbrain.utils.Utils;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import net.sf.json.JSONNull;
import net.sf.json.JSONObject;

import org.apache.log4j.Logger;
import org.javatuples.Pair;

public class MultiLangProcessTupleObserver implements MultiLangMessageHandler {

  private MultiLangProcess _proc;
  private LinkedBlockingQueue<Object> _queue;
  public static final Object DONE_MARKER = new Object();   //used to mark 'done' in the queue
  public static final Object END_CYCLE_MARKER = new Object();   //used to mark the end of a cycle
  private Operation _operation;
  private final long NEXT_TUPLE_TIMEOUT_CHECK = Config.getOrDefault("multilang.next.tuple.timeout.check", Utils.valueOf(1000L * 60 * 10)).longValue(); // 1000L * 30
  private boolean _watching;
  static Logger _log = Logger.getLogger(MultiLangProcessTupleObserver.class);
  
  public MultiLangProcessTupleObserver(MultiLangProcess proc, Operation op) {
    _proc = proc;
    _operation = op;
  }
  
  private void init() {
    _queue = new LinkedBlockingQueue<>();
  }
  
  
  public LinkedBlockingQueue<Object> queue() {
    return _queue;
  }
  
  public void startWatching() {
    init();
    _watching = true;
    _proc.addMessageListener(this);
  }
  
  
  
  /***
   * 
   * @param collector
   * @throws InterruptedException 
   */
  @Deprecated
  public MultiLangProcessTupleObserver collectTuplesUntilDone(OutputCollector collector) throws OperationException, InterruptedException {

    // Collect
    while( collectNextTuple(collector) ) {}
    
    // Done
    return this;
  }
  
  
  
  
  
  /****
   * @param collector
   * @return T if we have more tuples, F otherwise
   * @throws InterruptedException 
   */
  public boolean collectNextTuple(OutputCollector collector) throws OperationException, InterruptedException {

    // Collect
    if (_watching == false) throw new OperationException(_operation, "not watching");
    Object p = this.takeNextTuple();
    if (p == DONE_MARKER) {
      return false;
    } else if (p instanceof Pair) {
      final Pair<?, ?> pp = (Pair<?, ?>) p;
      final Object v0 = pp.getValue0();
      final Object v1 = pp.getValue1();
      if (v0 instanceof String && v1 instanceof MapTuple) {
        
        collector.emit( (String)v0, (MapTuple) v1);
        
        //_log.info(v1.toString());
        return true;
      }
    } else if (p instanceof Exception) {
      throw new OperationException(_operation, (Exception)p);
    }  
    throw new OperationException(_operation, "unknown type");
  }

  
  

  /**
   * @throws InterruptedException
   * @throws OperationException 
   * 
   */
  public MultiLangProcessTupleObserver waitForDoneMessageWithoutCollecting() throws InterruptedException, OperationException {
    
    // Collect
    if (_watching == false) throw new OperationException(_operation, "not watching");
    while(this.takeNextTuple() != DONE_MARKER) {
      // Do nothing. wait for null.
      throwUnhandledErrors();
    }
    
    // Done
    return this;
  }
  
  

  public MultiLangProcessTupleObserver stopWatching() {
    _watching = false;
    _proc.removeMessageListener(this);
    return this;
  }
  

  
  
  @Override
  public void handleMessage(String line) throws OperationException {
    
    Benchmark.markBegin("multilang.observer.handle_message");
    try { 
        
      // Sanity
      if (line == null) return;
      if (line.equals("end")) return;
      if (line.length() == 0) return;
      
      // Init
      JSONObject obj = JSONUtil.parseObj(line);
      // No command?
      if (obj.has("command") == false)
        return;
      
      // Emits?
      if (obj.getString("command").equalsIgnoreCase("emit")) {
        
        // Get the stream
        String streamName = obj.optString("stream", null);
        if (streamName == null || streamName.equals("null")) { // <- wierd json parse thing/bug
          try {
            streamName = _operation.defaultStream();
          } catch (DefaultStreamException e) {
            throw new OperationException(_operation, e);
          }
        }
        
        // Extract the tuple
        JSONObject jsonTuple = obj.getJSONObject("tuple"); 
        JSONObject meta = obj.optJSONObject("meta");
        if (meta == null || meta.isNullObject()) meta = new JSONObject();
        
        // Extract the meta
        MetaTuple metaTuple = new MetaTuple(meta.optLong("since", System.currentTimeMillis()), Double.valueOf(meta.optDouble("confidence", 1.0)), meta.optString("source", ""));
    
        // Build the final tuple
        MapTuple mapTuple = new MapTuple(metaTuple);
        
        // Get the values from the output json record
        for(Object field : jsonTuple.keySet()) {
          final String key = (String) field;
          if (key == null) {
            continue;
          }
          final Object object = jsonTuple.get(field);
          if (object == null) {
            final JSONNull nullObject = JSONNull.getInstance();
            assert (nullObject != null);
            mapTuple.add(key, nullObject);
          } else {
            mapTuple.add(key, object);
          }
        }
    
        // Done
        //_log.debug("queuing up tuple " +  mapTuple + " to stream: " + streamName);
        _queue.add(new Pair<>(streamName, mapTuple));
        
      }
      
      // End Cycle?
      if (obj.getString("command").equalsIgnoreCase("end_cycle")) {
        //_log.debug("end_cycle message received");
        _queue.add(END_CYCLE_MARKER);
      }
      
      // DONE?
      if (obj.getString("command").equalsIgnoreCase("done")) {
        //_log.debug("'done' message received");
        _queue.add(DONE_MARKER);
      }
      
      // Failure? 
      if (obj.getString("command").equalsIgnoreCase("fail")) {
        _queue.add(
            new MultiLangProcessException(_proc)
              .setUserMessage(obj.getString("msg"))
              .setInternalMessage(obj.getString("msg"))
            );
      }
    } finally {
      Benchmark.markEnd("multilang.observer.handle_message");
    }
    
  }
  
  
  /**
   * @return Next tuple; Null if 'done' has been processed
   * @throws InterruptedException 
   */
  public Object takeNextTuple(long timeout, TimeUnit unit) throws InterruptedException {
    Benchmark.markBegin("multilang.observer.take_next_tuple.poll");
    try { 
      return this._queue.poll(timeout, unit);
    } finally {
      Benchmark.markEnd("multilang.observer.take_next_tuple.poll");
    }
  }
  
  public Object takeNextTuple() throws InterruptedException, OperationException {
    if (_watching == false) throw new OperationException(_operation, "not watching");
    Object p;
    do {
      
      // Get the next value, waiting up to NEXT_TUPLE_TIMEOUT if necessary
      p = takeNextTuple(NEXT_TUPLE_TIMEOUT_CHECK, TimeUnit.MILLISECONDS);
      throwUnhandledErrors();
      
    } while (p == null && this._proc.isAlive());

    // If we timeout above and not in paused state, something is likely wrong.
    if (p == null) {
      throw new OperationException(_operation, "The process failed to emit a tuple or DONE signal in the timeout period");
    }
    // Done
    throwUnhandledErrors();
    return p;
  }
  private void throwUnhandledErrors() throws OperationException {
    // Do we have any unreported errors?
    for(MultiLangErrorHandler eh : this._proc.getErrorListeners()) {
      eh.mabyeThrowNextError();
    }
  }

}
