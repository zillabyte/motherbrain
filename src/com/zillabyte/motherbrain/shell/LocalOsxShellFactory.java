package com.zillabyte.motherbrain.shell;

import java.util.Map;

import com.google.common.collect.Maps;
import com.zillabyte.motherbrain.utils.Utils;

public class LocalOsxShellFactory implements ShellFactory {

  /**
   * 
   */
  private static final long serialVersionUID = 2534476521450901059L;

  @Override
  public Map<String, String> getEnvironment() {
    Map<String, String> m = Maps.newHashMap(System.getenv());
    m.put("PATH", "/Users/jake/zb1/cli/bin:/Users/jake/.rbenv/shims:/usr/bin:/bin:/usr/sbin:/sbin:/usr/local/bin:/opt/X11/bin:/Users/jake/bin:/Users/jake/zb1/fatty/bin:/Users/jake/zb1/infrastructure/bin:/Users/jake/zb1/motherbrain.jvm/bin:/Users/jake/zb1/zillalog/bin:/Users/jake/zb1/common/bin:/Users/jake/zb1/infrastructure/vagrant/gmb:/usr/local/share/npm/bin/:/Users/jake/bin/hadoop/bin:/Users/jake/bin/storm/bin");
    return m;
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
