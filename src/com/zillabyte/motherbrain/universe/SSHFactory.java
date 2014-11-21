package com.zillabyte.motherbrain.universe;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.ClosedByInterruptException;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;

import com.zillabyte.motherbrain.utils.Utils;

public abstract class SSHFactory implements Serializable{

  /**
   * 
   */
  private static final long serialVersionUID = 8641727369432892453L;

  public abstract void runCommands(String host, List<String> commands) throws InterruptedException;
  
  public void runCommands(String host, String... commands) throws InterruptedException {
    runCommands(host, Arrays.asList(commands));
  }

  
  public static class Vagrant extends SSHFactory {
    /**
     * 
     */
    private static final long serialVersionUID = 5350717827279099819L;
    static Logger log = Logger.getLogger(SSHFactory.class);
    private static final Local localFactory = new Local();
    
    @Override
    public void runCommands(String host, List<String> commands) throws InterruptedException {
      if (Utils.getHost() == host) {
        localFactory.runCommands(host, commands);
        return;
      }
      final String vagrantKey = "-----BEGIN RSA PRIVATE KEY-----\n"+
                                "MIIEogIBAAKCAQEA6NF8iallvQVp22WDkTkyrtvp9eWW6A8YVr+kz4TjGYe7gHzI\n"+
                                "w+niNltGEFHzD8+v1I2YJ6oXevct1YeS0o9HZyN1Q9qgCgzUFtdOKLv6IedplqoP\n"+
                                "kcmF0aYet2PkEDo3MlTBckFXPITAMzF8dJSIFo9D8HfdOV0IAdx4O7PtixWKn5y2\n"+
                                "hMNG0zQPyUecp4pzC6kivAIhyfHilFR61RGL+GPXQ2MWZWFYbAGjyiYJnAmCP3NO\n"+
                                "Td0jMZEnDkbUvxhMmBYSdETk1rRgm+R4LOzFUGaHqHDLKLX+FIPKcF96hrucXzcW\n"+
                                "yLbIbEgE98OHlnVYCzRdK8jlqm8tehUc9c9WhQIBIwKCAQEA4iqWPJXtzZA68mKd\n"+
                                "ELs4jJsdyky+ewdZeNds5tjcnHU5zUYE25K+ffJED9qUWICcLZDc81TGWjHyAqD1\n"+
                                "Bw7XpgUwFgeUJwUlzQurAv+/ySnxiwuaGJfhFM1CaQHzfXphgVml+fZUvnJUTvzf\n"+
                                "TK2Lg6EdbUE9TarUlBf/xPfuEhMSlIE5keb/Zz3/LUlRg8yDqz5w+QWVJ4utnKnK\n"+
                                "iqwZN0mwpwU7YSyJhlT4YV1F3n4YjLswM5wJs2oqm0jssQu/BT0tyEXNDYBLEF4A\n"+
                                "sClaWuSJ2kjq7KhrrYXzagqhnSei9ODYFShJu8UWVec3Ihb5ZXlzO6vdNQ1J9Xsf\n"+
                                "4m+2ywKBgQD6qFxx/Rv9CNN96l/4rb14HKirC2o/orApiHmHDsURs5rUKDx0f9iP\n"+
                                "cXN7S1uePXuJRK/5hsubaOCx3Owd2u9gD6Oq0CsMkE4CUSiJcYrMANtx54cGH7Rk\n"+
                                "EjFZxK8xAv1ldELEyxrFqkbE4BKd8QOt414qjvTGyAK+OLD3M2QdCQKBgQDtx8pN\n"+
                                "CAxR7yhHbIWT1AH66+XWN8bXq7l3RO/ukeaci98JfkbkxURZhtxV/HHuvUhnPLdX\n"+
                                "3TwygPBYZFNo4pzVEhzWoTtnEtrFueKxyc3+LjZpuo+mBlQ6ORtfgkr9gBVphXZG\n"+
                                "YEzkCD3lVdl8L4cw9BVpKrJCs1c5taGjDgdInQKBgHm/fVvv96bJxc9x1tffXAcj\n"+
                                "3OVdUN0UgXNCSaf/3A/phbeBQe9xS+3mpc4r6qvx+iy69mNBeNZ0xOitIjpjBo2+\n"+
                                "dBEjSBwLk5q5tJqHmy/jKMJL4n9ROlx93XS+njxgibTvU6Fp9w+NOFD/HvxB3Tcz\n"+
                                "6+jJF85D5BNAG3DBMKBjAoGBAOAxZvgsKN+JuENXsST7F89Tck2iTcQIT8g5rwWC\n"+
                                "P9Vt74yboe2kDT531w8+egz7nAmRBKNM751U/95P9t88EDacDI/Z2OwnuFQHCPDF\n"+
                                "llYOUI+SpLJ6/vURRbHSnnn8a/XG+nzedGH5JGqEJNQsz+xT2axM0/W/CRknmGaJ\n"+
                                "kda/AoGANWrLCz708y7VYgAtW2Uf1DPOIYMdvo6fxIB5i9ZfISgcJ/bbCUkFrhoH\n"+
                                "+vq/5CIWxCPp0f85R4qxxQ5ihxJ0YDQT9Jpx4TMss4PSavPaBH3RXow5Ohe+bYoQ\n"+
                                "NE5OgEXk2wVfZczCZpigBKbKZHNYcelXtTt/nP3rsCuGcM4h53s=\n"+
                                "-----END RSA PRIVATE KEY-----";
      
      final String vagrantFileName = "/tmp/vagrant_key";
      final File vagrantFile;
      try{
        vagrantFile = File.createTempFile(vagrantFileName, ".pem");
        try {
          BufferedWriter writer = new BufferedWriter(new FileWriter(vagrantFile));
          try {
            writer.append(vagrantKey);
          } finally {
            try {
              writer.close();
            } catch(ClosedByInterruptException e) {
              throw (InterruptedException) new InterruptedException().initCause(e);
            }
          }
          Utils.bash("chmod 600 " + vagrantFile.getAbsolutePath());      
          String fullCommand = "ssh -i " + vagrantFile.getAbsolutePath() +" -oStrictHostKeyChecking=no -oConnectTimeout=60 vagrant@" + host;
          String actualCommand = "";
          for(String command : commands) {
            if(actualCommand == "") {
              actualCommand += " \"sudo " + command;
            } else {
              actualCommand += " && sudo " + command;
            }
          }
          actualCommand += "\"";
          fullCommand += actualCommand;
          
          Utils.bash(fullCommand);
        } catch(ClosedByInterruptException e) {
          throw (InterruptedException) new InterruptedException().initCause(e);
        } finally {
          vagrantFile.delete();
        }
      } catch(IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
  
  
  
  public static class Local extends SSHFactory {
    /**
     * 
     */
    private static final long serialVersionUID = 7924760568870118001L;
    static Logger log = Logger.getLogger(SSHFactory.class);
    
    @Override
    public void runCommands(String host, List<String> commands) {
      String actualCommand = "";
      for(String command : commands) {
        if(actualCommand == "") {
          actualCommand += command;
        } else {
          actualCommand += " && " + command;
        }
      }
      
      Utils.bash(actualCommand);
    }
  }
  
  
  
  public static class AWS extends SSHFactory {
    /**
     * 
     */
    private static final long serialVersionUID = -8438946736747882279L;

    @Override
    public void runCommands(String host, List<String> commands) {
      String keyPath = "~/zb1/.credentials/ec2_2013_01.pem";
      String fullCommand = "chmod 400 " + keyPath + "; ssh -i " + keyPath + " -oStrictHostKeyChecking=no -oConnectTimeout=60 " + host;
      String actualCommand = "";
      for(String command : commands) {
        if(actualCommand == "") {
          actualCommand += " \"sudo " + command;
        } else {
          actualCommand += " && sudo " + command;
        }
      }
      actualCommand += "\"";
      fullCommand += actualCommand;
      
      Utils.bash(fullCommand);
    }
  }
  
}