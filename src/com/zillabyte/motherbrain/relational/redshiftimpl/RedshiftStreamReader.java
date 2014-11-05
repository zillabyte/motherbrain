package com.zillabyte.motherbrain.relational.redshiftimpl;

import java.sql.SQLException;
import java.util.List;

import org.apache.commons.lang.NotImplementedException;

import com.zillabyte.motherbrain.flow.MapTuple;
import com.zillabyte.motherbrain.relational.StreamMarker;
import com.zillabyte.motherbrain.relational.StreamReader;
import com.zillabyte.motherbrain.relational.naivepostgresimpl.NaivePostgresStreamMarker;

public class RedshiftStreamReader implements StreamReader {

  
  /**
   * 
   */
  private static final long serialVersionUID = -6018263262979356015L;
  private NaivePostgresStreamMarker _marker;  
  
  public final int LONG_SLEEP_TIME = 1000 * 10;
  public final int SHORT_SLEEP_TIME = 500;
  private long _count;



  public RedshiftStreamReader() {
    _marker = new NaivePostgresStreamMarker();
    _count = -1;
  }
  
  
  
  @Override
  public List<MapTuple> readBatch(long maxRecords) throws SQLException {
    throw new NotImplementedException("see SourceFromOffloadedRelation()");
  }
  
  
  @Override
  public List<MapTuple> readBatch() throws SQLException {
    return readBatch(1000);
  }



  @Override
  public void fastForward(StreamMarker marker) {
    if (marker == null) {
      _marker = new NaivePostgresStreamMarker();
    } else {
      _marker = (NaivePostgresStreamMarker) marker;
    }
  }



  @Override
  public StreamMarker currentMarker() {
    return _marker;
  }




  @Override
  public boolean hasSize() {
    return _count >= 0;
  }



  @Override
  public long size() {
    return _count ;
  }

  
  

}
