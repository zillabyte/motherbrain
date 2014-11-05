# The Coordinated Output Collector Survival Guide 


### Normal Operation

```                                                         
 +------------+  Send tuples         +------------+      
 |            | -------------------> |            |      
 |            |                      |            |      
 |            |                Acks  |            |      
 |            |  <-----------------+ |            |      
 |  Node 1    |                      |  Node 2    |      
 |            |  Batch Complete      |            |      
 |            | -------------------> |            |      
 |            |                      |            |      
 |            |         Batch Ack    |            |      
 |            | <------------------+ |            |      
 +------------+                      +------------+      
```

##### How do tuples fail?

Tuples `fail` when an `ack` has not been received from the downstream node within a given timeout (default 30s).

##### What happens when a tuple fails? 

This is up to the implementation of `SupportFactory`.  Currently, failed tuples just die (`DoNothingFailedTupleHandler`)

An ambitious engineer may implement a `ReplayFailedTupleHandler` and resend tuples into the stream. 

##### What is `BatchCompelete` and `BatchAck`?

The former signals to downstream tasks that the current batch is complete.  This is useful in RPC and aggregation scenarios.  The `BatchAck` is just like a `TupleAck`, and tells the upstream task everything is okay. 

##### What if a supervisor is down, or other fatal errors? 

In these scenarios, the *emitting* node will detect that no acks have come back. After a threshold has been reaches, the emitting node itself will start throwing errors.  The idea here is to **fail fast**, rather than try to work with a defunct machine. 


                                                         
