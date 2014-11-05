package grandmotherbrain.flow.rpc;

import grandmotherbrain.flow.MapTuple;

import java.util.Collection;
import java.util.LinkedList;

import org.eclipse.jdt.annotation.NonNullByDefault;
import org.javatuples.Pair;

@NonNullByDefault
public final class RPCResponse {

  final String id;
  final Collection<Pair<String, MapTuple>> _tuples;

  RPCResponse(final String id) {
    this.id = id;
    _tuples = new LinkedList<>();
  }
  
  public String getId() {
    return this.id;
  }

  public Collection<Pair<String, MapTuple>> getTuples() {
    return _tuples;
  }

  /**
   * This is not thread-safe, but this should be okay as long as it is only
   * ever called directly after creation, when the modifying thread has the
   * only reference to the response.
   *
   * <p>For this reason, it is package-scope, not public.
   */
  RPCResponse addTuple(Pair<String, MapTuple> tuple) {
    _tuples.add(tuple);
    return this;
  }

  /**
   * This is not thread-safe, but this should be okay as long as it is only
   * ever called directly after creation, when the modifying thread has the
   * only reference to the response.
   *
   * <p>For this reason, it is package-scope, not public.
   */
  RPCResponse addTuple(String sinkId, MapTuple tuple) {
    return addTuple(new Pair<>(sinkId, tuple));
  }

  public static RPCResponse create(String id) {
    RPCResponse req = new RPCResponse(id);
    return req;
  }

  @Override
  public String toString() {
    return "<" + this.id + " : " + this._tuples.toString() + ">";
  }
}
