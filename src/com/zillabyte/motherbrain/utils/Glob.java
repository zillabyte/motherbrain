package com.zillabyte.motherbrain.utils;

public class Glob {

  public static boolean matchesGlob(String glob, String val) {
    return val.matches(createRegexFromGlob(glob));
  }
  
  public static String createRegexFromGlob(String glob) {
      String out = "^";
      for(int i = 0; i < glob.length(); ++i)
      {
          final char c = glob.charAt(i);
          switch(c)
          {
          case '*': out += ".*"; break;
          case '?': out += '.'; break;
          case '.': out += "\\."; break;
          case '\\': out += "\\\\"; break;
          default: out += c;
          }
      }
      out += '$';
      return out;
  }
  
}
