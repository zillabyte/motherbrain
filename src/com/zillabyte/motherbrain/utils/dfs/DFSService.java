package com.zillabyte.motherbrain.utils.dfs;

import java.io.File;
import java.io.Serializable;
import java.util.List;

public interface DFSService extends Serializable {

  public void writeFile(String path, byte[] content);
  
  public List<String> listPath(String path);
  
  public byte[] readFile(String path);
  
  public boolean pathExists(String path);

  public void maybeCreateDirectory(String path);

  public void copyFile(File fromFile, String toFile);

  public void deleteFile(String path);

  public String getUriFor(String path);

}
