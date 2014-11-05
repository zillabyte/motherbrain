package com.zillabyte.motherbrain.flow.buffer;

import java.io.Serializable;

public interface BufferFlusher extends Serializable {

  void flushProducers(String sinkTopic);

}
