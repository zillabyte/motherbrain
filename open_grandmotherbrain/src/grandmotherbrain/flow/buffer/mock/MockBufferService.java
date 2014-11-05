package grandmotherbrain.flow.buffer.mock;

import grandmotherbrain.flow.buffer.BufferService;
import grandmotherbrain.relational.BufferQuery;

public class MockBufferService implements BufferService {
  //TODO: Fix for tests.
  
  @Override
  public void maybeFillBuffer(BufferQuery query) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void sinkBuffer(String _topicName, String shardPath, String shardPrefix, String bucket) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public boolean hasTopic(String topic) {
    return true; //why not.
  }

  
  @Override
  public void init() {
  }

  @Override
  public void shutDown() {    
  }

  
  
  

}
