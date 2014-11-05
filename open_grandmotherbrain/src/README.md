# GrandMotherBrain 

## Patterns 

While browsing the GrandmotherBrain, keep in mind these patterns.  It will reduce insanity.

### Interface & Implementation 

Much of the code in GMB has an interface with an underlying implementation.  When you see this pattern, a few things should register: 

* The author expects multiple implementations, possibly for different use cases.  A good example here is the `FlowService`.  Currently, the underlying implementation is `StormFlowService`, but in the future we may write our own `AkkaFlowService` etc. 

* The author was too lazy to build the proper implementation, and the interface is used as a communication tool.   The `PostgresRelationalService` is a good example here. I (jake) am unable to write the FDW Postgres implementation.  To communicate to Josh what I'm looking for, I created the `RelationalService` and he can add his own implementation. 

### Factories 

Allowing for different implementations can create lots of gotchas when instantiating new objects.  For example, We have a generalized `Flow` interface.  But, depending on the `FlowService` implementation we use, the `Flow` object may need to be backed up by Storm or Akka, etc.   In other words, we need an easy way to create `StormFlows` or `AkkaFlows`

This is what Factories are good at.  In the flow example, we have a `FlowFactory` interface that can be implemented by `StormFlowFactory`.  This allows the user to switch between different implementations very easily. 



## Services 

Currently, GrandmotherBrain will operate in a single JVM (except for Storm, which manages itself).  It is likely in the future that the services that 
make up GrandMotherBrain will need to be distributed to different machines.  

### TopService 

TopService is the top of the service hierarchy.  Functionally, it is responsible for recieving flows and delegating them to sub-services. (i.e. push it down to the PostgresService, etc).  

### FlowService (Storm)

The FlowService is an abstraction of Storm (Trident).  It really provides two useful functions: `registerFlow` for starting new flows on the Storm cluster, and `getStateOf` to simply query the state of the flow. 

Why make this an abstraction? Because the day will come when we want to dump storm and possibly build our own implementation.  By drawing this abstraction, we are no longer locked into Storm going forward. 



### RelationalService (Postgres)

RelationService is an abstraction on top of Postgres.  It provides two interesting methods: `createStreamQuery` and `streamInsertHandler`.  These methods are two sides of the same coin: stream data out of Postgres, and stream data into Postgres. 

Note that interface requires nothing of FDWs or Buffers.  Thus, the underyling implementation is free to use FDWs/Buffers however it wants.  The implementation SHOULD expect to handle arbitrary HUGE amounts of data.  As long as that data can be piped through `createStreamQuery` and `streamInsertHandler`, then it should play seamlesly well with Storm. 

Pointer: The implementation will be stateful (i.e. tracking the size of buffers, etc).  The implementation should use `Universe` to capture state. 


## Major Concepts & Models 

### Universe

The universe object is a singleton (i.e. it only has one instance in the entire JVM).  It is meant to capture the state of the entire GrandmotherBrain system.  I.e. it should be used to track which Flows are running, their underlying buffers, provisions. etc.  The goal is to be able to terminate the entire system, then restart in the same state.  


### Flow / FlowInstance

A `Flow` is the most common model in Grandmotherbrain.  It's really just a generalization of Functions, Spouts, Sinks, and Toplogies.  Note the `Flow` is a DSL construct and is useful only when wiring up a toplogy. 

A `FlowInstance` is the representation of an actual `Flow` running on a cluster. 

### Tuple / RichTuple

A `Tuple` is comprised of two parts: `Values` and `Meta`. `Values` contains the user-defined values, `Meta` contains the meta data for the tuple.  Currently, we have three meta fields: since, source, and confidence.  This may grow in the future. 

New in GrandmotherBrain is the concept of `RichTuple`.  The goal of `RichTuple` is to be a MAP, instead of just a LIST.  Hopefully this will make it easier for us to add new types of data and still be backwards compatible with existing datasets. 


### ResultStream

A `ResultStream` is the main bridge between Postgres and Storm in GrandmotherBrain.  It continually reads all data from a given query (SXP expression).  That data is used as a Spout in the Storm topology. 


# Terminology 

Motherbrain | Storm
--- | ---
Operation | Component 
Instance | Task 
Flow (App) | Topology
Component | [nil]


# Where To Start 

Start by looking at the tests.  (SimpleWebCrawlerTests).



