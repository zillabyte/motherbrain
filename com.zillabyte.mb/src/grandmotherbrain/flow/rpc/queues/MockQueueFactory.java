package grandmotherbrain.flow.rpc.queues;

import grandmotherbrain.flow.MapTuple;
import grandmotherbrain.flow.rpc.RPCRequest;
import grandmotherbrain.flow.rpc.RPCResponse;
import grandmotherbrain.flow.rpc.RPCSink;
import grandmotherbrain.flow.rpc.RPCSource;
import grandmotherbrain.universe.Universe;
import grandmotherbrain.utils.JSONUtil;
import grandmotherbrain.utils.SerializableMonitor;
import grandmotherbrain.utils.Utils;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import net.sf.json.JSONObject;

import org.apache.log4j.Logger;
import org.eclipse.jdt.annotation.NonNullByDefault;
import org.eclipse.jdt.annotation.Nullable;
import org.javatuples.Pair;

@NonNullByDefault
public final class MockQueueFactory implements QueueFactory {

  private static final long serialVersionUID = 8009800068003744010L;

  final LinkedBlockingQueue<RPCRequest> _inputs = new LinkedBlockingQueue<>();
  final ConcurrentHashMap<String, List<MapTuple>> _outputs = new ConcurrentHashMap<>();
  final SerializableMonitor outputMonitor;
  static final Logger _log = Utils.getLogger(MockQueueFactory.class);
  
  public MockQueueFactory() {
    outputMonitor = new SerializableMonitor();
  }

  
  /***
   * 
   * @author jake
   *
   */
  public final class Input implements InputQueue {

    /**
     * Serialization ID
     */
    private static final long serialVersionUID = -9068751590216675151L;

    @Override
    public RPCRequest getNextRequest() throws InterruptedException {
      final RPCRequest request = _inputs.poll();
      return request;
    }

    @Override
    public void init() {
      if(Universe.instance().env().isLocal()) {
        JSONObject rpcArgs = JSONUtil.parseObj((String)Universe.instance().config().getOrException("rpc.args"));
        MockQueueFactory queue = (MockQueueFactory) Universe.instance().rpcQueueFactory();
        // Queue up rpc args
        Set<String> rpcInputStreams = rpcArgs.keySet();
        for(String inputStream : rpcInputStreams) {
          Integer count = 0;
          RPCRequest req = RPCRequest.create("request-"+count);
          Iterator<?> i = rpcArgs.getJSONArray(inputStream).iterator();

          while(i.hasNext()) {
            JSONObject inputStreamArg = (JSONObject) i.next();
            _log.info("Adding "+inputStreamArg+" to input queue.");
            req.addTuple(inputStream, MapTuple.create(inputStreamArg));
          }
          try {
            queue.addInput(req);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }
    }

    @Override
    public void shutdown() {
      // Do nothing
    }

    @Override
    public boolean nextRequestAvailable() {
      return !_inputs.isEmpty();
    }
  }

  
  /***
   * 
   * @author jake
   *
   */
  public final class Output implements OutputQueue {

    /**
     * 
     */
    private static final long serialVersionUID = 1586591978391066585L;

    @Override
    public void init() {
      // Do nothing
    }

    @Override
    public void shutdown() {
      // Do nothing
    }

    @Override
    public void sendResponse(RPCResponse response) {
      _log.info("response added: " + response);
      synchronized(outputMonitor) {
        if (_outputs.contains(response.getId()) == false) {
          _outputs.put(response.getId(), new LinkedList<MapTuple>());
        }
        List<MapTuple> out = _outputs.get(response.getId());
        for (final Pair<String, MapTuple> pair : response.getTuples()) {
          out.add(pair.getValue1());
        }
        outputMonitor.notifyAll();
      }
    }
  }

  
  /***
   * 
   */
  @Override
  public InputQueue getInputQueue(RPCSource source) {
    return new Input();
  }

  
  /***
   * 
   */
  @Override
  public OutputQueue getOutputQueue(RPCSink sink) {
    return new Output();
  }

  
  /***
   * 
   * @param request
   */
  public void addInput(RPCRequest request) throws InterruptedException {
    _inputs.put(request);
  }

  
  /***
   * 
   * @param id
   */
  public List<MapTuple> getResponse(String id) {
    return _outputs.get(id);
  }

  
  /***
   * 
   * @param id
   * @param size
   * @param maxWait
   * @return
   * @throws InterruptedException
   */
  public boolean waitForResponse(final String id, final int size, final long maxWait) throws InterruptedException {
    final long start = System.currentTimeMillis();
    synchronized (outputMonitor) {
      while (true) {
        // First check if timeout has expired... 
        long waitTime = maxWait - (System.currentTimeMillis() - start);
        if (waitTime <= 0) {
          _log.info("timeout exceeded (" + maxWait + " " + waitTime + "). _outputs: " + _outputs);
          return false;
        }
        // Then check if we are free to exit... 
        List<MapTuple> array = this.getResponse(id);
        if (array != null && array.size() == size) {
          return true;
        }
        // System.err.println("output queue wait: " + waitTime);
        outputMonitor.wait(waitTime);
      }
    }
  }

  /***
   * 
   * @param tuple
   */
  public boolean outputContains(final String uuid, final @Nullable MapTuple tuple) {
    List<MapTuple> out = this._outputs.get(uuid);
    if (out != null) {
      for (final MapTuple t : out) {
        if (t.equalsTuple(tuple)) {
          return true;
        }
      }
    }
    return false;
  }

  public void clear() {
    // System.err.println("clear input queue");
    _outputs.clear();
    _inputs.clear();
  }

}
