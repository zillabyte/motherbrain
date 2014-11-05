package com.zillabyte.motherbrain.shell;

import java.util.Map;

import com.zillabyte.motherbrain.utils.Utils;

public class UbuntuEc2ShellFactory implements ShellFactory {

  /**
   * 
   */
  private static final long serialVersionUID = -6695528080258162102L;

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
