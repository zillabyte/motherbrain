package com.zillabyte.motherbrain.top;

import java.io.IOException;

import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.zillabyte.motherbrain.universe.Config;
import com.zillabyte.motherbrain.universe.LocalUniverseBuilder;

public class LocalCommandLineHelper {

  
  
  @SuppressWarnings("static-access")
  public static void createUniverse(String[] args) throws ParseException, IOException, InterruptedException {

    // Command line options
    CommandLineParser parser = new GnuParser();
    Options availOptions = new Options();
    Config config = new Config();

    
    ///////////////////////////////////////////////////////
    // OPTIONS ////////////////////////////////////////////
    ///////////////////////////////////////////////////////

    availOptions.addOption(OptionBuilder
        .hasArg()
        .withLongOpt("flow.class")
        .withType(String.class)
        .create()
        );
    
    availOptions.addOption(OptionBuilder
        .hasArg()
        .withLongOpt("flow.name")
        .withType(String.class)
        .create()
        );
    
    availOptions.addOption(OptionBuilder
        .hasArg()
        .withLongOpt("directory")
        .withType(String.class)
        .create()
        );
    
    availOptions.addOption(OptionBuilder
        .hasArg()
        .withLongOpt("rpc.args")
        .withType(String.class)
        .create()
        );
    
    availOptions.addOption(OptionBuilder
        .hasArg()
        .withLongOpt("api.host")
        .withType(String.class)
        .create()
        );
    
    availOptions.addOption(OptionBuilder
        .hasArg()
        .withLongOpt("api.port")
        .withType(Number.class)
        .create()
        );
    
    availOptions.addOption(OptionBuilder
        .hasArg()
        .withLongOpt("output.prefix")
        .withType(String.class)
        .create()
        );

    ///////////////////////////////////////////////////////
    // BUILD CORE CONFIG //////////////////////////////////
    ///////////////////////////////////////////////////////
    
    // Parse the options...
    final org.apache.commons.cli.CommandLine rawOptions = parser.parse(availOptions, args);
    assert (rawOptions != null);
    final CommandLine options = new CommandLine(rawOptions);
    
    // Help?
    if (options.hasOption("help")) {
      System.out.println(availOptions.toString());
      System.exit(1);
      return;
    }
    
    
    // Core Config
    config.put("flow.class",   options.getOptionValue("flow.class", "app"));
    config.put("flow.name",    options.getOptionValue("flow.name", "local_app"));
    config.put("directory",    options.getOptionValue("directory", "."));
    config.put("rpc.args",     options.getOptionValue("rpc.args", ""));
    config.put("api.host",     options.getOptionValue("api.host", "localhost"));
    config.put("api.port",     Integer.parseInt(options.getOptionValue("api.port", "5000")));
    config.put("output.prefix",  options.getOptionValue("output.prefix", ""));

    ///////////////////////////////////////////////////////
    // CREATE THE UNIVERSE ////////////////////////////////
    ///////////////////////////////////////////////////////
    
    // Inform the user of our config..
    System.out.println(config.toString());
    
    LocalUniverseBuilder.createDefaultLocal(config);
    
  }
  
  
  
  public static void printObligatoryCoolBanner() {
    System.out.println(""
        + "" +
        "______  ___       _____ ______                ______                  _____         \n" + 
        "___   |/  /______ __  /____  /_ _____ ___________  /_ ______________ ____(_)_______ \n" + 
        "__  /|_/ / _  __ \\_  __/__  __ \\_  _ \\__  ___/__  __ \\__  ___/_  __ `/__  / __  __ \\\n" + 
        "_  /  / /  / /_/ // /_  _  / / //  __/_  /    _  /_/ /_  /    / /_/ / _  /  _  / / /\n" + 
        "/_/  /_/   \\____/ \\__/  /_/ /_/ \\___/ /_/     /_.___/ /_/     \\__,_/  /_/   /_/ /_/ \n" + 
        "                                                                                    "
        );
        
  }
  
  
  
  
}
