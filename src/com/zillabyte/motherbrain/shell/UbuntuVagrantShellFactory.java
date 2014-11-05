package com.zillabyte.motherbrain.shell;

import java.util.Map;

import com.zillabyte.motherbrain.utils.Utils;

public class UbuntuVagrantShellFactory implements ShellFactory {

  /**
   * 
   */
  private static final long serialVersionUID = 4164129526919849450L;

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
    return "/usr/bin/";
  }
  
}
