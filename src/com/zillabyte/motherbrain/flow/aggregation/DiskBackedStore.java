package com.zillabyte.motherbrain.flow.aggregation;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Stack;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.log4j.Logger;
import org.codehaus.plexus.util.FileUtils;

import com.google.common.base.Throwables;
import com.google.common.io.Files;
import com.zillabyte.motherbrain.flow.MapTuple;
import com.zillabyte.motherbrain.flow.operations.AggregationOperation;
import com.zillabyte.motherbrain.universe.Universe;
import com.zillabyte.motherbrain.utils.Utils;


/***
 *  Production Aggregate Store
 *  @author sjarvie
 */
public class DiskBackedStore implements AggregationStore {

  private static final long serialVersionUID = -1837276500478667648L;
  private AggregationOperation _operation;
  private String _extraPrefix;
  private static Logger _log = Utils.getLogger(DiskBackedStore.class);


  /***
   * 
   * @param o
   */
  public DiskBackedStore(AggregationOperation o) {
    this(o, "");
  }

  /***
   * 
   * @param o
   * @param extraPrefix
   */
  public DiskBackedStore(AggregationOperation o, String extraPrefix) {
    _operation = o;
    _extraPrefix = extraPrefix;

    // Make sure the target is clear... 
    File root = new File(rootPath());
    if (root.exists()) {
      _log.warn("aggregation root already exists: " + root + " ... deleting ");
      try {
        FileUtils.deleteDirectory(root);
      } catch (IOException e) {
        Throwables.propagate(e);
      }
    }
  }


  /***
   * 
   * @return
   */
  public String rootPath() {
    String root = Universe.instance().fileFactory().getTmp().toString();
    String flowId = _operation.topFlowId();
    String instanceName = _operation.instanceName();
    String extraPrefix = (_extraPrefix.equals("") ? "/" : _extraPrefix + "/");
    return root + "/f" + flowId + "/" + instanceName + "/aggregate_tuples/" + extraPrefix;
  }


  /**
   * @return the location of the tuple aggregate keys and values
   */
  public String dataPath(Object batchId){
    return rootPath() + batchId ;
  }


  /**
   * Version 1
   * Traverses to locate a key's directory
   * @return t
   */
  public String keyPath(Object batchId, AggregationKey key) {
    String md5 = DigestUtils.md5Hex(Utils.serialize(key));
    return dataPath(batchId) + "/" + md5;
  }


  @Override
  public void addToGroup(Object batchId, AggregationKey key, MapTuple tuple) {


    // add key if necessary
    String path = keyPath(batchId, key);

    try {
      File keyFile = new File(path + "/key.txt");
      if (!keyFile.exists()) {
        Files.createParentDirs(keyFile);
        Files.touch(keyFile);
        Files.write(Utils.serialize(key), keyFile);
      }

      TuplePage page = new TuplePage(path);
      page.insert(tuple);

    } catch (IOException e) {
      e.printStackTrace();
    }
  }


  @Override
  public boolean hasGroup(Object batch, AggregationKey key) {
    File f = new File(keyPath(batch, key));
    return f.exists() && f.isDirectory() && (f.listFiles() != null);
    // return _map.containsKey(key);
  }


  @Override
  public Iterator<MapTuple> getGroupIterator(Object batch, AggregationKey key) {
    // return a custom Iterator that incrementally reads the next tuples
    TuplePage page = new TuplePage(keyPath(batch, key));
    return page.iterator();
  }


  @Override
  public void deleteGroup(Object batch, AggregationKey key) {
    File keyDir = new File(keyPath(batch, key));
    if (keyDir.exists()) {
      for (File f : keyDir.listFiles()){
        f.delete();
      }
      keyDir.delete();
    }
  }


  @Override
  public Iterator<AggregationKey> keyIterator(final Object batch) throws AggregationException {



    Iterator<AggregationKey> it = new Iterator<AggregationKey>() {

      private AggregationKey _currentKey = null;

      // Use DFS to locate key files
      private Stack<File> _stack = null;
      private String root = dataPath(batch);


      @Override
      public boolean hasNext() {

        // Initialize the DFS
        if (_stack == null) {
          _stack = new Stack<File>();
          File rootDir = new File(root);
          _stack.add(rootDir);
        }

        if (_currentKey != null){
          return true;
        }

        if (_stack.empty()){
          return false;
        } else {

          // Find a valid key if it exists
          while (_stack.empty()){
            File dir = _stack.pop();

            if (dir.exists() && dir.isDirectory()) {
              for (File f : dir.listFiles()){
                if (f.isFile() && f.getName().equals("key.txt")){
                  try {
                    AggregationKey key = (AggregationKey) Utils.deserialize(Files.toByteArray(f));
                    _currentKey = key;
                    return true;
                  } catch (IOException e) {
                    return false;
                  }
                } else if (f.isDirectory()){
                  _stack.add(f);
                }
              } 
            }
          }

        }
        return false;
      }

      @Override
      public AggregationKey next() {
        if (!hasNext()){
          return null;
        }

        AggregationKey key = _currentKey;
        _currentKey = null;        
        return key;
      }

      @Override
      public void remove() {}
    };
    return it;
  }


  @Override
  public void flush(Object batch) {    
  }


  @Override
  public void deleteBatch(Object batch) throws AggregationException {
    try {
      FileUtils.deleteDirectory(new File(dataPath(batch)));
    } catch (IOException e) {
      throw new AggregationException(e);
    }
  }

}
