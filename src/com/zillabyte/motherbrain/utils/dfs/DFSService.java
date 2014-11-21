package com.zillabyte.motherbrain.utils.dfs;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;

public interface DFSService extends Serializable {

  public void writeFile(String path, byte[] content) throws IOException;
  
  public List<String> listPath(String path) throws IOException;
  
  public byte[] readFile(String path) throws IOException;
  
  public boolean pathExists(String path) throws IOException;

  public void maybeCreateDirectory(String path) throws IOException;

  public void copyFile(File fromFile, String toFile) throws IOException;

  public void deleteFile(String path) throws IOException;

  public String getUriFor(String path) throws IOException;

}
