package grandmotherbrain.relational;

import grandmotherbrain.flow.MapTuple;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.List;


/**
 * A `ResultStream` is the main bridge between Postgres and Storm in GrandmotherBrain.  It continually 
 * reads all data from a given query (SXP expression).  That data is used as a Spout in the 
 * Storm topology. 
 *
 */
public interface StreamReader extends Serializable {

//  public void start(ResultsHandler handler);
//  public static interface ResultsHandler {
//    public void newResults(ResultSet resultSet, long sequence);
//  }
  
  public List<MapTuple> readBatch(long maxRecords) throws SQLException;
  public List<MapTuple> readBatch() throws SQLException;
  
  
  
  /***
   * If the stream is finite, then this should return true;
   */
  public boolean hasSize();
  
  /***
   * For finite streams, returns the total number of rows in this stream. 
   */
  public long size();
  
  
  /***
   * Fast forwards to the give marker
   * @param marker
   */
  public void fastForward(StreamMarker marker);
  
  /**
   * Gets the current marker
   */
  public StreamMarker currentMarker();
  
  

  

}
