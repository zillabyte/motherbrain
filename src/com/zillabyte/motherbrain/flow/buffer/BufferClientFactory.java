package com.zillabyte.motherbrain.flow.buffer;

import com.zillabyte.motherbrain.flow.operations.LoopException;



/**
 * 
 * @author sashi
 *
 */
public interface BufferClientFactory {

  public BufferConsumer createConsumer(SourceFromBuffer operation) throws LoopException;
  
  public BufferProducer createProducer(SinkToBuffer operation) throws LoopException;

  public BufferFlusher createFlusher();
  
}

