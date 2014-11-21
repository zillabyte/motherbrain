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
  
  private File prefixify(String path) throws IOException {
    if(path.startsWith("/tmp") || path.startsWith("/mnt")) {
      Utils.TODO("if you see this error, go bug jake about it.");
    }
    File f = new File(_root, path);
    Files.createParentDirs(f);
    return f;
  }
    
  @Override
  public void writeFile(String path, byte[] content) throws IOException {
    File f = prefixify(path);
    System.err.println(f.getAbsolutePath());

    FileOutputStream output = new FileOutputStream(f);
    IOUtils.write(content, output);
  }

  @Override
  public List<String> listPath(String path) throws IOException {
    File dir = prefixify(path);
    File[] dirFiles = dir.listFiles();
    List<String> stringFiles = Lists.newArrayList();
    for(File f : dirFiles) {
      stringFiles.add(f.getName());
    }
    return stringFiles;
  }
  
  
  @Override
  public byte[] readFile(String path) throws IOException {
    FileInputStream input = new FileInputStream(prefixify(path));
    return IOUtils.toByteArray(input);
  }
  


  @Override
  public boolean pathExists(String path) throws IOException {
    File file = prefixify(path);
    if(file.exists()) return true;
    return false;
  }

  

  @Override
  public void maybeCreateDirectory(String path) throws IOException {
    File dir = prefixify(path);
    dir.mkdirs();
  }
  


  @Override
  public void copyFile(File fromFile, String toFile) throws IOException {
    FileUtils.copyFile(fromFile, prefixify(toFile));
  }
  
 
  
  @Override
  public void deleteFile(String path) throws IOException {
    File file = prefixify(path);
    file.delete();
  }

  @Override
  public String getUriFor(String path) throws IOException {
    return "file://" + prefixify(path).getAbsolutePath();
  }
  

}
