package com.zillabyte.motherbrain.utils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import org.apache.commons.io.IOUtils;

public class UrlHelper {

  // http://www.regexplanet.com/advanced/java/index.html
  // (http(s)?://)?(\w+@)?[\w\.\-]*?(([\w\-]+\.[\w\-]+\.\w{2})|([\w\-]+\.\w{2,4}))($|/|\s)(.*)
	public static String v = "(http(s)?://)?(\\w+@)?[\\w\\.\\-]*?(([\\w\\-]+\\.[\\w\\-]+\\.\\w{2})|([\\w\\-]+\\.\\w{2,4}))($|/|\\s)(.*)";

  // http://www.regexplanet.com/advanced/java/index.html
  // (http(s)?://)?((\w+@)?[\w\.\-]*?(([\w\-]+\.[\w\-]+\.\w{2})|([\w\-]+\.\w{2,4})))($|/|\s)(.*)
  static String fullString = "(http(s)?://)?((\\w+@)?[\\w\\.\\-]*?(([\\w\\-]+\\.[\\w\\-]+\\.\\w{2})|([\\w\\-]+\\.\\w{2,4})))($|/|\\s)(.*)";

  final static Pattern pattern;
  final public static Pattern fullPattern;
	
	static {
	  final Pattern _pattern = Pattern.compile(v);
	  assert(_pattern != null);
	  pattern = _pattern;

	  final Pattern _fullPattern = Pattern.compile(fullString);
	  assert(_fullPattern != null);
	  fullPattern = _fullPattern;
	}

	
  public static @Nullable String getHost(String url) {
  	Matcher m = pattern.matcher(url.trim());
  	if (m.matches()) {
  		return m.group(4);
  	}
    return null;
  }
  
  
  public static @Nullable String getHostFull(String url) {
    Matcher m = fullPattern.matcher(url.trim());
    if (m.matches()) {
      return m.group(3).replace("www.", "");
    }
    return null;
  }


  public static String fetchWebBody(String rawUrl) throws IOException {
      
    // Logger.debug("Fetching: " + rawUrl);
    URL url = new URL(rawUrl);
    URLConnection con = url.openConnection();
    con.setConnectTimeout(10 * 1000);
    con.setReadTimeout(15 * 1000);
    
    try{
      if (con.getContentType() == null) {
        throw new IOException("can't connect: " + rawUrl);
      }
      if (con.getContentType().contains("text/html")) {
        // success
      } else if (con.getContentType().contains("text/plain")) {
        // success
      } else if (con.getContentType().contains("application/json")) {
        // success
      } else {
        throw new IOException("not text/html " + con.getContentType() + " : " + rawUrl);
      }
    }catch(IllegalArgumentException e){
      throw new IOException("bad url: " + rawUrl);
    }
    
    
    final InputStream in = con.getInputStream();
    try {
      String encoding = con.getContentEncoding();
      encoding = encoding == null ? "UTF-8" : encoding;
      String body = IOUtils.toString(in, encoding);
      assert (body != null);
      return body;
    } finally {
      in.close();
    }    
  }
  	
  	
}
