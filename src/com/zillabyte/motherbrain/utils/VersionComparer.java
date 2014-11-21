package com.zillabyte.motherbrain.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class VersionComparer {

  
  private static Pattern versionPattern = Pattern.compile("(\\d+)\\.(\\d+)\\.(\\d+)");
  
  public static boolean isAtLeast(String test, String minimum) {
    
    if (test == null) return false;
    
    Matcher testMatch = versionPattern.matcher(test);
    if (testMatch == null) return false;
    if (testMatch.find() == false) return false;
    int testMajor = Integer.parseInt(testMatch.group(1));
    int testMinor = Integer.parseInt(testMatch.group(2));
    int testPatch = Integer.parseInt(testMatch.group(3));
    
    Matcher minMatch = versionPattern.matcher(minimum);
    if (minMatch.find() == false) throw new RuntimeException("You should pass in a valid 'minimum' parameter.");
    int minMajor = Integer.parseInt(minMatch.group(1));
    int minMinor = Integer.parseInt(minMatch.group(2));
    int minPatch = Integer.parseInt(minMatch.group(3));
    
    if (testMajor >= minMajor) {
      if (testMinor >= minMinor) {
        if (testPatch >= minPatch) {
          return true;
        }
      }
    }
    
    return false;
  }
  
  
}
