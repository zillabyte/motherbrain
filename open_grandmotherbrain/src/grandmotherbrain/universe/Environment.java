package grandmotherbrain.universe;

import java.io.Serializable;

public class Environment implements Serializable {
  
  
  /**
   * 
   */
  private static final long serialVersionUID = -6275065801726560620L;
  private String _name;

  public Environment(String name) {
    super();
    _name = name;
  }
  
  public String name() {
    return _name;
  }
  
  
  @Override
  public String toString() {
    return name();
  }
  

  public static Environment devForMe() {
    return Environment.dev(System.getProperty("user.name"));
  }
  
  public static Environment mockForMe() {
    return Environment.mock(System.getProperty("user.name"));
  }
  
  public static Environment dev(String suffix) {
    return new Environment("dev-" + suffix);
  }
  
  public static Environment mock(String suffix) {
    return new Environment("mock-" + suffix);
  }

  public static Environment test() {
    return new Environment("test");
  }

  public static Environment local() {
    return new Environment("local");
  }
  
  public boolean isTest() {
    return _name.equals("test");
  }
  
  public boolean isProd() {
    return _name.equals("prod") || _name.equals("production");
  }
  
  public boolean isLocal() {
    return _name.equals("local");
  }
  
  public boolean isTestOrProd() {
    return isTest() || isProd();
  }

}
