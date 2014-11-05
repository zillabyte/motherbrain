package com.zillabyte.motherbrain.flow.collectors.coordinated.support.naive;

import com.zillabyte.motherbrain.flow.collectors.coordinated.CoordinatedOutputCollector;
import com.zillabyte.motherbrain.flow.collectors.coordinated.support.CoordinatedOutputCollectorSupportFactory;
import com.zillabyte.motherbrain.flow.collectors.coordinated.support.FailedTupleHandler;
import com.zillabyte.motherbrain.flow.collectors.coordinated.support.TupleIdGenerator;
import com.zillabyte.motherbrain.flow.collectors.coordinated.support.TupleIdMapper;
import com.zillabyte.motherbrain.flow.collectors.coordinated.support.TupleIdSet;

public class NaiveCoordinatedOutputCollectorSupportFactory implements CoordinatedOutputCollectorSupportFactory {

  /**
   * 
   */
  private static final long serialVersionUID = 6266634691810249265L;

  @Override
  public TupleIdSet createTupleIdSet(CoordinatedOutputCollector op) {
    return new UncompressedTupleIdSet();
  }

  @Override
  public TupleIdGenerator createTupleIdGenerator(CoordinatedOutputCollector op) {
    return new SerialTupleIdGenerator(op);
  }

  @Override
  public FailedTupleHandler createFailedTupleHandler(CoordinatedOutputCollector op) {
    return new DoNothingFailedTupleHandler(op);
  }

  @Override
  public TupleIdMapper createTupleIdMapper(CoordinatedOutputCollector _collector) {
    return new UncompressedTupleIdMapper();
  }

}
