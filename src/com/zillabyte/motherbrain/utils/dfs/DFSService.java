package com.zillabyte.motherbrain.utils.dfs;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.jets3t.service.S3ServiceException;

import com.zillabyte.motherbrain.universe.S3Exception;


public interface DFSService extends Serializable {

  public void writeFile(String path, byte[] content) throws IOException, S3Exception, InterruptedException;
  
  public List<String> listPath(String path) throws S3ServiceException, S3Exception;
  
  public byte[] readFile(String path) throws IOException, S3Exception;
  
  public boolean pathExists(String path) throws S3Exception;

  public void maybeCreateDirectory(String path) throws IOException, S3Exception, InterruptedException;

  public void copyFile(File fromFile, String toFile) throws S3Exception, IOException;

  public void deleteFile(String path) throws S3Exception;

  public String getUriFor(String path);

}
