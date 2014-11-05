package com.zillabyte.motherbrain.flow.collectors.coordinated.support.naive;

import org.apache.log4j.Logger;

import com.zillabyte.motherbrain.flow.MapTuple;
import com.zillabyte.motherbrain.flow.collectors.OutputCollector;
import com.zillabyte.motherbrain.flow.collectors.coordinated.CoordinatedOutputCollector;
import com.zillabyte.motherbrain.flow.collectors.coordinated.support.FailedTupleHandler;
import com.zillabyte.motherbrain.flow.collectors.coordinated.support.TupleIdSet;
import com.zillabyte.motherbrain.utils.MeteredLog;

public class DoNothingFailedTupleHandler implements FailedTupleHandler {

  /**
   * 
   */
  private static final long serialVersionUID = -646225163323102206L;
  
  private static Logger _log = Logger.getLogger(DoNothingFailedTupleHandler.class);
  private CoordinatedOutputCollector _col;
  
  public DoNothingFailedTupleHandler(CoordinatedOutputCollector o) {
    _col = o;
  }
  
  @Override
  public void handleFailedTupleIds(OutputCollector collector, Object batch, TupleIdSet tupleIds) {
    MeteredLog.info(_log, "failed tuples for batch: " + batch + ": failed count:" + tupleIds.size() + " operation: " + _col.getOperation().instanceName(), 2000);
    _col.debug("(cont) failed tuples for batch: " + tupleIds);
  }

  @Override
  public void observeTupleIdMapping(Object tupleId, MapTuple tuple) {
    // Do nothing
  }


}
