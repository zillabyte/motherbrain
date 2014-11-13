package com.zillabyte.motherbrain.flow.graph;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.eclipse.jdt.annotation.NonNullByDefault;
import org.javatuples.Pair;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.monitoring.runtime.instrumentation.common.com.google.common.collect.Maps;
import com.zillabyte.motherbrain.flow.Flow;
import com.zillabyte.motherbrain.flow.operations.Operation;
import com.zillabyte.motherbrain.flow.operations.Sink;
import com.zillabyte.motherbrain.flow.operations.Source;


@NonNullByDefault
@SuppressWarnings("unchecked")
public final class FlowGraph implements Serializable {

  private static final long serialVersionUID = -1340686782939393035L;
  private Collection<Connection> _arcs;
  private Map<String, Operation> _idMap;

  
  /****
   * 
   * @param flow
   */
  public FlowGraph() {
    _arcs = new ArrayList<>();
    _idMap = new HashMap<>();
  }
  
  
  
  /***
   * 
   * @param source
   */
  public boolean contains(String source, String dest) {
    for(Connection c : _arcs) {
      if (c.sourceId().equals(source) && c.destId().equals(dest)) {
        return true;
      }
    }
    return false;
  }
  
  public boolean contains(Operation source, Operation dest) {
    return contains(source.namespaceName(), dest.namespaceName());
  }
  
  public boolean isLoopBack(String source, String dest) {
    for(Connection c : _arcs) {
      if (c.sourceId().equals(source) && c.destId().equals(dest) && c.loopBack()) {
        return true;
      }
    }
    return false;
  }
  
  public boolean isLoopBack(String stream) {
    for(Connection c : _arcs) {
      if(c.streamName().equals(stream) && c.loopBack()) return true;
    }
    return false;
  }
  
  
  /***
   * 
   * @param source
   * @param dest
   * @return 
   */
  public Connection connect(Operation source, Operation dest, String name, Boolean loopBack, Integer maxIter) {   
    if (contains(source, dest)) throw new RuntimeException("connection already exists: " + source.operationId() + " -> " + dest.operationId());
    
    if (source.namespaceName().equals(dest.namespaceName()) && !loopBack) throw new RuntimeException("cannot connect operation to itself: " + source + " -> " + dest);
    
    Connection c = new Connection(this, source, dest, name, loopBack, maxIter);
    
    _arcs.add(c);
    _idMap.put(source.namespaceName(), source);
    _idMap.put(dest.namespaceName(), dest);
    
    return c;
  }
  
  public Connection connect(Operation source, Operation dest, String name) {
    return connect(source, dest, name, false, 0);
  }
  
  public Set<String> operationsBetween(final Operation source, final Operation dest) {
    return operationsBetween(source.namespaceName(), dest.namespaceName());
  }

  public Set<String> operationsBetween(final String source, final String dest) {
    // This queue will track the operation as well as all previous seen operations in the path in the Set
    ConcurrentLinkedQueue<Pair<String, Set<String>>> queue = new ConcurrentLinkedQueue<Pair<String, Set<String>>>();
    queue.add(new Pair<String, Set<String>>(source, new HashSet<String>()));
    Set<String> opsBetween = Sets.newHashSet();
    Map<String, LinkedList<Set<String>>> searchEndOperation = Maps.newHashMap();
    
    while(!queue.isEmpty()) {
      Pair<String, Set<String>> opPair = queue.remove();
      String op = opPair.getValue0();
      Set<String> opList = opPair.getValue1();
      
      if(opList.contains(op)) {
        // If the previously seen operations already contains this operation, then we're going to start going down the same path we
        // previous went down. 
        if(source.equals(dest)) {
          // In the case where source == dest, we need to add all of the seen operations to the final list before we move on (the 
          // source is put in the list first, so the next time we see it, it should be in the list already).
          opsBetween.addAll(opList);
        } else {
          // If the source != dest, we need to account for the possibility that the end-operation is somehow connected to the dest
          // somewhere further down. We add the current seen operations to a linked list and store the end operation-linked list
          // pair in a hashmap (note that the linked list is just a list of the parents of the end-operation). Later we see if any 
          // of the end-operations in the hashmap are linked to the dest.
          LinkedList<Set<String>> otherEndLists;
          if(searchEndOperation.containsKey(op)) {
            otherEndLists = searchEndOperation.get(op);
          } else {
            otherEndLists = Lists.newLinkedList();
          }
          otherEndLists.add(opList);
          searchEndOperation.put(op, Lists.newLinkedList(otherEndLists));
        }
        continue;
      } else if (op.equals(dest) && !source.equals(dest)) {
        // If we have not already seen the operation and the source and dest are different, then we have also arrived at an end-point
        // and should add all of the seen operations to the final list. Note that the condition source != dest is necessary because on
        // the first iteration in the source == dest case, the first condition is satisfied, but we do not want to move to the next
        // element in this case (we want to explore the graph!).
        opsBetween.addAll(opList);
        continue;
      } else {
        // Otherwise, we add the operation to the seen list and move on.
        opList.add(op);
      }            
      
      // Add next connections to the queue.
      for(Connection c : connectionsFrom(op)) {
        queue.add(new Pair<String, Set<String>>(c.dest().namespaceName(), Sets.newHashSet(opList)));
      }
    }
    
    // Check to see if any of the end-operations are somehow linked to the dest. If they are, we need to also add all operations in
    // the parents of the end-operation to opsBetween.
    for(String op : searchEndOperation.keySet()) {
      if(opsBetween.contains(op)) {
        for(Set<String> so : searchEndOperation.get(op)) {
          opsBetween.addAll(so);
        }
      }
    }
    
    // Don't include the destination itself.
    if(source.equals(dest)) opsBetween.remove(source);
    return opsBetween;
  }
  
  /***
   * 
   * @param source
   */
  public List<Connection> loopConnectionsFrom(final String source) {
    List<Connection> list = Lists.newLinkedList();
    for(Connection c : this._arcs) {
      if (c.source().namespaceName().equals(source) && c.loopBack()) {
        list.add(c);
      }
    }
    return list;
  }
  
  /***
   * 
   * @param source
   */
  public List<Connection> nonLoopConnectionsFrom(final String source) {
    List<Connection> list = Lists.newLinkedList();
    for(Connection c : this._arcs) {
      if (c.source().namespaceName().equals(source) && !c.loopBack()) {
        list.add(c);
      }
    }
    return list;
  }
  
  public List<Connection> nonLoopConnectionsFrom(final Operation source) {
    return nonLoopConnectionsFrom(source.namespaceName());
  }
  
  /***
   * 
   * @param dest
   */
  public List<Connection> nonLoopConnectionsTo(final String dest) {
    List<Connection> list = Lists.newLinkedList();
    for(Connection c : this._arcs) {
      if (c.dest().namespaceName().equals(dest) && !c.loopBack()) {
        list.add(c);
      }
    }
    return list;
  }
  
  public List<Connection> nonLoopConnectionsTo(final Operation dest) {
    return nonLoopConnectionsTo(dest.namespaceName());
  }
  
  public List<Connection> connectionsTo(final Operation dest) {
    return connectionsTo(dest.namespaceName());
  }
  
  public List<Connection> connectionsTo(final String dest) {
    List<Connection> list = Lists.newLinkedList();
    for(Connection c : this._arcs){
      if(c.dest().namespaceName().equals(dest)) {
        list.add(c);
      }
    }
    return list;
  }
  
  public List<Connection> connectionsFrom(final Operation source) {
    return connectionsFrom(source.namespaceName());
  }
  
  public List<Connection> connectionsFrom(final String source) {
    List<Connection> list = Lists.newLinkedList();
    for(Connection c : this._arcs){
      if(c.source().namespaceName().equals(source)) {
        list.add(c);
      }
    }
    return list;
  }

  public Collection<Operation> allOperations() {
    final Collection<Operation> operations = this._idMap.values();
    return operations == null ? Collections.EMPTY_SET : operations;
  }

  public Operation getById(String id) {
    return this._idMap.get(id);
  }
  

  public Operation getById(Operation o) {
    return getById(o.namespaceName());
  }
  

  public <T extends Operation> Collection<T> getByTypeAndContainer(Class<T> klass, Flow container) {
    Set<T> ret = new HashSet<>();
    for(Operation o : this._idMap.values()) {
      if (o.getContainerFlow() == container) {
        if (klass.isInstance(o)) {
          ret.add((T)o);
        }
      }
    }
    return ret;
  }

  
  
  public <T extends Operation> Collection<T> getByType(Class<T> klass) {
    Set<T> ret = new HashSet<>();
    for(Operation o : this._idMap.values()) {
      if (klass.isInstance(o)) {
        ret.add((T)o);
      }
    }
    return ret;
  }

  
  public Set<Operation> operationsTo(Operation op) {
    Set<Operation> set = new HashSet<>();
    for(Connection p : this.connectionsTo(op)) {
      set.add(this.getById(p.sourceId()));
    }
    return set;
  }
  
  public Set<Operation> operationsFrom(Operation op) {
    Set<Operation> set = new HashSet<>();
    for(Connection p : this.connectionsFrom(op)) {
      set.add(this.getById(p.destId()));
    }
    return set;
  }

  public Set<Operation> nonLoopOperationsTo(Operation op) {
    Set<Operation> set = new HashSet<>();
    for(Connection p : this.nonLoopConnectionsTo(op)) {
      set.add(this.getById(p.sourceId()));
    }
    return set;
  }
  
  
  public Set<Operation> nonLoopOperationsFrom(Operation op) {
    Set<Operation> set = new HashSet<>();
    for(Connection p : this.nonLoopConnectionsFrom(op)) {
      set.add(this.getById(p.destId()));
    }
    return set;
  }



  public List<String> streamsFrom(String opId) {
    List<String> list = Lists.newLinkedList();
    for(Connection c : this._arcs) {
      if (c.source().namespaceName().equals(opId)) {
        list.add(c.streamName());
      }
    }
    return list;
  }
  
  public List<String> streamsTo(String opId) {
    List<String> list = Lists.newLinkedList();
    for(Connection c : this._arcs) {
      if (c.dest().namespaceName().equals(opId)) {
        list.add(c.streamName());
      }
    }
    return list;
  }
  
  public List<String> streamsFrom(Operation op) {
    return streamsFrom(op.namespaceName());
  }
  
  public List<String> streamsTo(Operation op) {
    return streamsTo(op.namespaceName());
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for(Connection c : _arcs) {
      sb.append(c.sourceId() + " -> " + c.destId() + " (" + c.streamName() + ")\n");
    }
    final String string = sb.toString();
    assert (string != null);
    return string;
  }
  
  
  public void debug() {
    System.err.println(this.toString());
  }


  
  public void inject(String prefix, FlowGraph graph) {
    for(Operation o : graph._idMap.values()) {
      o.addNamespacePrefix(prefix);
    }
    for(Connection c : graph._arcs) {
      
      // Init 
      Operation s = c.source();
      Operation d = c.dest();
      String name = c.streamName();
      Boolean lb = c.loopBack();
      Integer mi = c.maxIterations();
      name = prefix + "." + name;
      
      // Connect
      c = connect(s, d, name, lb, mi);
    }
  }

  
  public Collection<Source> sources() {
    List<Source> list = Lists.newLinkedList();
    for(Operation o : this._idMap.values()) {
      if (o instanceof Source) {
        list.add((Source) o);
      }
    }
    return list;
  }
  
  public Collection<Sink> sinks() {
    List<Sink> list = Lists.newLinkedList();
    for(Operation o : this._idMap.values()) {
      if (o instanceof Sink) {
        list.add((Sink) o);
      }
    }
    return list;
  }


  /***
   * Does the graph have any cycles/loopbacks? 
   */
  public boolean hasLoopbacks() {
    
    for(Connection c : this._arcs) {
      if (c.loopBack()) {
        return true;
      }
    }
    return false;
  }


  public boolean contains(Operation op) {
    return this.allOperations().contains(op);
  }

  public boolean containsAny(Collection<Operation> allOperations) {
    for(Operation o : allOperations) {
      if (contains(o)) return true;
    }
    return false;
  }



  /***
   * Removes an operation and connects the remaining arcs together... 
   */
  public void pluck(Operation o) {
    
    // Init
    List<Connection> originalIncoming = this.connectionsTo(o);
    List<Connection> originalOutgoing = this.connectionsFrom(o);
    
    // PLUCK
    String id = getIdByOperation(o);
    this._idMap.remove(id);
    for(Connection c : Lists.newArrayList(this._arcs)) {
      if (c.destId().equals(id) || c.sourceId().equals(id)) {
        _arcs.remove(c);
      }
    }
    
    // Cross connect
    for(Connection s : originalIncoming) {
      for(Connection d : originalOutgoing) {
        Connection n = new Connection(this, s.source(), d.dest(), s.streamName(), d.loopBack(), d.maxIterations());
        _arcs.add(n);
      }
    }
  }

  
  
  private String getIdByOperation(Operation o) {
    for(Entry<String, Operation> e : this._idMap.entrySet()) {
       if (e.getValue() == o) {
         return e.getKey();
       }
    }
    return null;
  }



  public Connection getConnectionByStream(String streamName) {
    for(Connection c : this._arcs) {
      if (c.streamName().equals(streamName)) return c;
    }
    return null;
  }


  
  
}
