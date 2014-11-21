package com.zillabyte.motherbrain.utils.dfs;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.io.IOUtils;

public class DFSServiceWrapper implements DFSService {

  /**
   * 
   */
  private static final long serialVersionUID = 2979302072474849989L;
  
  private DFSService _delegate;
  
  public DFSServiceWrapper(DFSService delegate) {
    _delegate = delegate;
  }
  
  @Override
  public void writeFile(String path, byte[] content) throws IOException {
    _delegate.writeFile(path, content);
  }

  public void writeFile(String path, String content) throws IOException  {
    writeFile(path, content.getBytes(Charset.forName("UTF-8")));
  }
  
  public void writeFileGzip(String path, byte[] content) throws IOException {
    // Compress the content
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();

    GZIPOutputStream gzip = new GZIPOutputStream(buffer);
    gzip.write(content);
    gzip.close();
    
    // Write it
    writeFile(path, buffer.toByteArray());
  }
  
  public void writeFileGzip(String path, String content) throws IOException {
    writeFileGzip(path, content.getBytes(Charset.forName("UTF-8")));
  }
  

  @Override
  public List<String> listPath(String path) throws IOException {
    return _delegate.listPath(path);
  }

  
  @Override
  public byte[] readFile(String path) throws IOException {
    return _delegate.readFile(path);
  }

  public String readFileAsString(String path) throws IOException {
    return new String(readFile(path));
  }
  
  
  public byte[] readGzipFile(String path) throws IOException {
    byte[] content = _delegate.readFile(path);
    
    // Convert content to gzip input
    ByteArrayInputStream buffer = new ByteArrayInputStream(content);
    GZIPInputStream gzip = new GZIPInputStream(buffer);

    // Convert gzipped input back into bytes
    return IOUtils.toByteArray(gzip);
  }

  public String readGzipFileAsString(String path) throws IOException {
    return new String(readGzipFile(path));
  }
  
  
  @Override
  public boolean pathExists(String path) throws IOException {
    return _delegate.pathExists(path);
  }

  @Override
  public void maybeCreateDirectory(String path) throws IOException {
    _delegate.maybeCreateDirectory(path);
  }

  public void maybeCreateDirectory(File path) throws IOException {
    maybeCreateDirectory(path.getAbsolutePath());
  }
  
  
  public DFSService getDelegate() {
    return _delegate;
  }
  @Override
  public void copyFile(File fromFile, String toFile) throws IOException {
    _delegate.copyFile(fromFile, toFile);
  }

  @Override
  public void deleteFile(String path) throws IOException {
    _delegate.deleteFile(path);
  }
  
  public void deleteFile(File file) throws IOException {
    deleteFile(file.getAbsolutePath());
  }

  @Override
  public String getUriFor(String path) throws IOException {
    return _delegate.getUriFor(path);
  }


}
