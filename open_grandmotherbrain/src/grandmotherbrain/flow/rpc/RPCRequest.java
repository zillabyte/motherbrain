package grandmotherbrain.flow.rpc;

import grandmotherbrain.flow.MapTuple;

import java.util.Collection;
import java.util.LinkedList;

import org.eclipse.jdt.annotation.NonNullByDefault;
import org.javatuples.Pair;

@NonNullByDefault
public final class RPCRequest {

  public final String id;
  final Collection<Pair<String, MapTuple>> tuples;

  RPCRequest(final String id) {
    this.tuples = new LinkedList<>();
    this.id = id;
  }

  public Collection<Pair<String, MapTuple>> getTuples() {
    return tuples;
  }
  
  public RPCRequest addTuple(Pair<String, MapTuple> tuple) {
    tuples.add(tuple);
    return this;
  }
  
  public RPCRequest addTuple(String inputId, MapTuple tuple) {
    return addTuple(new Pair<>(inputId, tuple));
  }

  
  public static RPCRequest create(final String id) {
    RPCRequest req = new RPCRequest(id);
    return req;
  }
  
  
  @Override
  public String toString() {
    return "<id: " + id + " tuples: " + tuples + ">";
  }

}
