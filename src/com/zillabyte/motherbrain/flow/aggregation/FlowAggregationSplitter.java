package com.zillabyte.motherbrain.flow.aggregation;

import java.util.LinkedList;

import org.eclipse.jdt.annotation.NonNullByDefault;
import org.javatuples.Pair;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Lists;
import com.zillabyte.motherbrain.flow.App;
import com.zillabyte.motherbrain.flow.operations.AggregationOperation;
import com.zillabyte.motherbrain.flow.operations.GroupBy;
import com.zillabyte.motherbrain.flow.operations.Join;
import com.zillabyte.motherbrain.flow.operations.Operation;
import com.zillabyte.motherbrain.flow.operations.Sink;
import com.zillabyte.motherbrain.flow.operations.Source;

@NonNullByDefault
public final class FlowAggregationSplitter {
  
  
  
  public static LinkedListMultimap<AggregationOperation, Operation> getAggregationPredecessors(App flow) {
    
    // Init 
    final LinkedListMultimap<AggregationOperation, Operation> ret = LinkedListMultimap.create();
    /*
     * Just calls constructor.
     */
    assert (ret != null);
    
    // Prime the queue...
    LinkedList<Pair<AggregationOperation, Operation>> queue = Lists.newLinkedList();
    for(Operation o : flow.getOperations()) {
      if (o != null && o instanceof Sink) {
        queue.add(new Pair<AggregationOperation, Operation>(null, o));
      }
    }
    
    // BFS 
    while(queue.size() > 0) {
      
      // Init
      Pair<AggregationOperation, Operation> p = queue.removeFirst();
      AggregationOperation tailAgg = p.getValue0();
      Operation thisOp = p.getValue1();
      
      if (thisOp instanceof Join) {
        
        Join joiner = (Join) thisOp;
        queue.add(new Pair<AggregationOperation, Operation>(joiner, joiner.lhsPrevOperation()));
        queue.add(new Pair<AggregationOperation, Operation>(joiner, joiner.rhsPrevOperation()));
        queue.add(new Pair<>(tailAgg, joiner.lhsPrevOperation()));
        queue.add(new Pair<>(tailAgg, joiner.rhsPrevOperation()));
        ret.put(joiner, joiner.lhsPrevOperation());
        ret.put(joiner, joiner.rhsPrevOperation());
        
      } else if (thisOp instanceof GroupBy) {
        
        GroupBy grouper = (GroupBy) thisOp;
        queue.add(new Pair<AggregationOperation, Operation>(grouper, grouper.prevNonLoopOperation()));
        queue.add(new Pair<>(tailAgg, grouper.prevNonLoopOperation()));
        ret.put(grouper, grouper.prevNonLoopOperation());
        
      } else if (thisOp instanceof AggregationOperation) {
        
        throw new RuntimeException("Unknown aggregation type "+thisOp.type());
        
      } else if (thisOp instanceof Source) {
        
        // Source, do nothing.. 
        
      } else if (tailAgg != null) {
        
        // We already have an aggregation for the tail, keep it going.. 
        queue.add(new Pair<>(tailAgg, thisOp.prevNonLoopOperation()));
        ret.put(tailAgg, thisOp.prevNonLoopOperation());
        
      } else {
        
        // No aggregator, so just keep traversing backwards
        queue.add(new Pair<AggregationOperation, Operation>(null, thisOp.prevNonLoopOperation()));
        
      } 
    }
    
    // Done
    return ret;
    
  }
  
  
  
  
  
}
