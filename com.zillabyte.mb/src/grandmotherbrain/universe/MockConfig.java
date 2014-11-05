package grandmotherbrain.universe;

import org.eclipse.jdt.annotation.NonNullByDefault;

@NonNullByDefault
public final class MockConfig extends Config {
  
  
  /**
   * 
   */
  private static final long serialVersionUID = -7632746694721403449L;

  /***
   * 
   * @param k
   * @param v
   */
  public static void set(final String k, final Object v) {
    Config c = Universe.instance().config();
    c.setInternal(k, v);
  }


}
