package com.zillabyte.motherbrain.flow.aggregation;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

import org.xerial.snappy.Snappy;

import com.google.common.io.Files;
import com.zillabyte.motherbrain.flow.MapTuple;
import com.zillabyte.motherbrain.utils.Utils;

/**
 * Manages the writing of tuples to a single file on disk
 * Serizlized tuple objects are written to disk in the following format
 * 
 *    +--------------------------------------------------------------------+
 *    |[LEN1][              TUPLE 1                   ][LEN2][  TUPLE2     |
 *    |    ][LEN3][    TUPLE3      ][LEN4] [       TUPLE 4    ] ........   |
 *    |                                                                    |
 *    |                                                                    |
 *    +--------------------------------------------------------------------+    
 *        
 * The lengths are fixed width 32 bit integers, denoting the size of the proceeding
 * serialized tuple object.
 * 
 * @author sjarvie
 *
 */
public class TuplePage implements Iterable<MapTuple>{
  private MapTuple[] arrayList;

  private String _keyPath;
  private File _tupleFile;
  private String _tupleFileName;


  public TuplePage(String keyPath){
    _keyPath = keyPath;
    _tupleFileName = _keyPath + "/tuple_data.ser";
    _tupleFile = new File(_tupleFileName);
    try {
      Files.createParentDirs(_tupleFile);
      Files.touch(_tupleFile);
    } catch (IOException e) {
      e.printStackTrace();
    }

  }

  public int readInt(byte[] buffer) {
    int b1 = (buffer[0] & 0xFF) << 24;
    int b2 = (buffer[1] & 0xFF) << 16;
    int b3 = (buffer[2] & 0xFF) << 8;
    int b4 = buffer[3] & 0xFF;
    return b1 | b2 | b3 | b4;
  }


  /**
   * Insert a tuple into the page, appending its serialized length to the file beforehand
   */
  public void insert(MapTuple tuple){

    try {

      // Generate serial data and length
      byte[] serialData = Snappy.compress(Utils.serialize(tuple));
      byte[] serialLen = ByteBuffer.allocate(4).putInt(serialData.length).array();


      // Append compressed objects to the file
      FileOutputStream fos = new FileOutputStream(_tupleFileName, true);
      fos.write(serialLen);
      fos.write(serialData);
      fos.close();

    } catch (IOException e) {
      e.printStackTrace();
    }
  }


  /**
   * 
   * @return
   */
  public Iterator<MapTuple> iterator(){

    Iterator<MapTuple> it = new Iterator<MapTuple>() {

      private FileInputStream fis;
      private int nextTupleLen = -1;

      @Override
      public boolean hasNext() {
        try {
          if (fis == null){
            fis = new FileInputStream(_tupleFile);
          }

          // If the next tuple length has not been fetched, try fetching it
          if (nextTupleLen == -1) {

            // Read an integer
            byte[] serialLenBytes = new byte[4];
            if (fis.read(serialLenBytes) != 4) {
              return false;
            }

            // Found a new serial tuple length
            nextTupleLen = readInt(serialLenBytes);
            return true;

          } else {
            return true;
          }


        } catch (IOException e) {
          e.printStackTrace();
        } 

        return false;
      }

      @Override
      public MapTuple next() {
        try {
          if (hasNext()){
            byte[] buffer = new byte[nextTupleLen];
            if (fis.read(buffer) < nextTupleLen) return null;
            nextTupleLen = -1;
            return (MapTuple)Utils.deserialize(Snappy.uncompress(buffer));

          }
        } catch (IOException e) {
          e.printStackTrace();
        }
        return null;
      }


      @Override
      public void remove(){}

    };
    return it;
  }    

}
