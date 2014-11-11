package com.zillabyte.motherbrain.flow.buffer;

import com.zillabyte.motherbrain.relational.BufferQuery;

public interface BufferService {

  public void maybeFillBuffer(BufferQuery query);

  public void sinkBuffer(BufferTopic _topic, String shardPath, String shardPrefix, String bucket);
  
  public boolean hasTopic(BufferTopic _topic);
  
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
