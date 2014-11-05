package com.zillabyte.motherbrain.shell;

import java.io.Serializable;
import java.util.Map;

public interface ShellFactory extends Serializable {
  
  Map<String, String>  getEnvironment();

  String cliDirectory();

  String lxcDirectory();
  
}
