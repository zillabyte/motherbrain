package com.zillabyte.motherbrain.utils;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;
import org.zeroturnaround.zip.NameMapper;
import org.zeroturnaround.zip.ZipUtil;

import com.google.common.io.Files;

public class CompressUtils {

  private static Logger _log = Logger.getLogger(CompressUtils.class);
  

  /****
   * 
   * @param root
   * @throws IOException
   */
  public static byte[] compress(File root) throws IOException {
    if (root.exists()) {
      File zip = new File(Files.createTempDir(), "data.zip");
      
      ZipUtil.pack(root, zip, new NameMapper() {
        @Override
        public String map(String name) {
          // Filter out the socket files... 
          if (name.endsWith(".sock"))
            return null;
          else 
            return name;
        }
      });
          
      byte[] data = Files.toByteArray(zip);
      zip.delete();
      return data;
    } else {
      _log.warn("no data to compress: " + root.toString());
      return new byte[] {};
    }
  }
  
  
  /***
   * 
   * @param data
   * @param dest
   * @throws IOException 
   */
  public static void decompress(byte[] data, File dest) throws IOException {
    if (data == null || data.length == 0) {
      _log.warn("no data to decompress to " + dest.toString());
    } else {
      File zip = new File(Files.createTempDir(), "data.zip");
      Files.write(data, zip);
      ZipUtil.unpack(zip, dest);
    }
  }
  
}
