package com.zillabyte.motherbrain.utils.dfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.codehaus.plexus.util.FileUtils;

import com.google.common.collect.Lists;
import com.google.monitoring.runtime.instrumentation.common.com.google.common.base.Throwables;
import com.google.monitoring.runtime.instrumentation.common.com.google.common.io.Files;
import com.zillabyte.motherbrain.utils.Utils;



public class LocalDFSService implements DFSService{
  
  /**
   * 
   */
  private static final long serialVersionUID = 8576730718155455692L;
  private static final String LOCAL_DFS_ROOT = "/tmp/local_dfs";
  private File _root;
  
  
  public LocalDFSService(String uniqueSuffix) {
    _root = new File(LOCAL_DFS_ROOT + uniqueSuffix);
    _root.mkdirs();
  }
  public LocalDFSService() {
    this("");
  }
  
  private File prefixify(String path) {
    if(path.startsWith("/tmp") || path.startsWith("/mnt")) {
      Utils.TODO("if you see this error, go bug jake about it.");
    }
    File f = new File(_root, path);
    try {
      Files.createParentDirs(f);
    } catch (IOException e) {
      Throwables.propagate(e);
    }
    return f;
  }
    
  @Override
  public void writeFile(String path, byte[] content) {
    File f = prefixify(path);
    System.err.println(f.getAbsolutePath());
    try {
      FileOutputStream output = new FileOutputStream(f);
      IOUtils.write(content, output);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<String> listPath(String path) {
    File dir = prefixify(path);
    File[] dirFiles = dir.listFiles();
    List<String> stringFiles = Lists.newArrayList();
    for(File f : dirFiles) {
      stringFiles.add(f.getName());
    }
    return stringFiles;
  }
  
  
  @Override
  public byte[] readFile(String path) {
    try {
      FileInputStream input = new FileInputStream(prefixify(path));
      return IOUtils.toByteArray(input);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  


  @Override
  public boolean pathExists(String path) {
    File file = prefixify(path);
    if(file.exists()) return true;
    return false;
  }

  

  @Override
  public void maybeCreateDirectory(String path) {
    File dir = prefixify(path);
    dir.mkdirs();
  }
  


  @Override
  public void copyFile(File fromFile, String toFile) {
    try {
      FileUtils.copyFile(fromFile, prefixify(toFile));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  
 
  
  @Override
  public void deleteFile(String path) {
    File file = prefixify(path);
    file.delete();
  }

  @Override
  public String getUriFor(String path) {
    return "file://" + prefixify(path).getAbsolutePath();
  }
  

}
