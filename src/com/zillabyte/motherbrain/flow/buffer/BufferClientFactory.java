package com.zillabyte.motherbrain.flow.buffer;

import com.zillabyte.motherbrain.flow.operations.OperationException;



/**
 * 
 * @author sashi
 *
 */
public interface BufferClientFactory {

  public BufferConsumer createConsumer(SourceFromBuffer operation) throws OperationException;
  
  public BufferProducer createProducer(SinkToBuffer operation) throws OperationException;

  public BufferFlusher createFlusher();
  
}

