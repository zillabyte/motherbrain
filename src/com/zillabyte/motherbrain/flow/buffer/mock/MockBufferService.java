package com.zillabyte.motherbrain.flow.buffer.mock;

import com.zillabyte.motherbrain.flow.buffer.BufferService;
import com.zillabyte.motherbrain.flow.buffer.BufferTopic;
import com.zillabyte.motherbrain.relational.BufferQuery;

public class MockBufferService implements BufferService {
  //TODO: Fix for tests.
  
  @Override
  public void maybeFillBuffer(BufferQuery query) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void sinkBuffer(BufferTopic _topicName, String shardPath, String shardPrefix, String bucket) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public boolean hasTopic(BufferTopic topic) {
    return true; //why not.
  }

  
  @Override
  public void init() {
  }

  @Override
  public void shutDown() {    
  }

  
  
  

}
