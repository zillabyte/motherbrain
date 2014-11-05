package grandmotherbrain.shell;

import grandmotherbrain.utils.Utils;

import java.util.Map;

public class UbuntuTeamCityShellFactory implements ShellFactory {

  /**
   * 
   */
  private static final long serialVersionUID = 1540203042756552583L;

  @Override
  public Map<String, String> getEnvironment() {
    return System.getenv();
  }

  @Override
  public String cliDirectory() {
    return Utils.expandPath("~/zb1/cli/bin");
  }

  @Override
  public String lxcDirectory() {
    return null;
  }
  
  
}
