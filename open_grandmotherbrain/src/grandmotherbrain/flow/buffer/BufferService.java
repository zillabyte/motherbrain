package grandmotherbrain.flow.buffer;

import grandmotherbrain.relational.BufferQuery;

public interface BufferService {

  public void maybeFillBuffer(BufferQuery query);

  public void sinkBuffer(String _topicName, String shardPath, String shardPrefix, String bucket);
  
  public boolean hasTopic(String topic);
  
//  public List<String> getSeedBrokers();
//
//  public String getZKConnectString();
//  
//  public String getZKConnectString(String namespace);
//
//  public ZkClient createGmbZkClient();

  public void init();

  public void shutDown();

//  public ZkClient createKafkaZkClient();
}
