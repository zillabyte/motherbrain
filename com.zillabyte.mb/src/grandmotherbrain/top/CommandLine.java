package grandmotherbrain.top;

import java.util.Iterator;
import java.util.List;

import org.apache.commons.cli.Option;
import org.eclipse.jdt.annotation.NonNullByDefault;
import org.eclipse.jdt.annotation.Nullable;


/**
 * A properly annotated variant of org.apache.commons.cli.CommandLine.
 */
@NonNullByDefault
@SuppressWarnings("unchecked")
public final class CommandLine {
  /**
   * Delegate commandLine.
   */
  final org.apache.commons.cli.CommandLine commandLine;

  CommandLine(final org.apache.commons.cli.CommandLine commandLine) {
    this.commandLine = commandLine;
  }
  
  /** 
   * Query to see if an option has been set.
   *
   * @param opt Short name of the option
   * @return true if set, false if not
   */
  public boolean hasOption(final String opt)
  {
      return commandLine.hasOption(opt);
  }

  /** 
   * Query to see if an option has been set.
   *
   * @param opt character name of the option
   * @return true if set, false if not
   */
  public boolean hasOption(final char opt)
  {
      return commandLine.hasOption(opt);
  }

  /**
   * Return the <code>Object</code> type of this <code>Option</code>.
   *
   * @param opt the name of the option
   * @return the type of this <code>Option</code>, or null if not found.
   */
  public @Nullable Object getOptionObject(final String opt)
  {
      return commandLine.getOptionObject(opt);
  }

  /**
   * Return the <code>Object</code> type of this <code>Option</code>.
   *
   * @param opt the name of the option
   * @return the type of opt, or null if not found.
   */
  public @Nullable Object getOptionObject(final char opt)
  {
      return commandLine.getOptionObject(opt);
  }

  /** 
   * Retrieve the argument, if any, of this option.
   *
   * @param opt the name of the option
   * @return Value of the argument if option is set, and has an argument,
   * otherwise null.
   */
  public @Nullable String getOptionValue(final String opt)
  {
      return commandLine.getOptionValue(opt);
  }

  /** 
   * Retrieve the argument, if any, of this option.
   *
   * @param opt the character name of the option
   * @return Value of the argument if option is set, and has an argument,
   * otherwise null.
   */
  public @Nullable String getOptionValue(final char opt)
  {
      return commandLine.getOptionValue(opt);
  }

  /** 
   * Retrieves the array of values, if any, of an option.
   *
   * @param opt string name of the option
   * @return Values of the argument if option is set, and has an argument,
   * otherwise null.
   */
  public @Nullable String[] getOptionValues(final String opt)
  {
      return commandLine.getOptionValues(opt);
  }

  /** 
   * Retrieves the array of values, if any, of an option.
   *
   * @param opt character name of the option
   * @return Values of the argument if option is set, and has an argument,
   * otherwise null.
   */
  public @Nullable String[] getOptionValues(final char opt)
  {
      return commandLine.getOptionValues(opt);
  }

  /** 
   * Retrieve the argument, if any, of an option.
   *
   * @param opt name of the option
   * @param defaultValue is the default value to be returned if the option 
   * is not specified
   * @return Value of the argument if option is set, and has an argument,
   * otherwise <code>defaultValue</code>.
   */
  public String getOptionValue(final String opt, final String defaultValue)
  {
      final String answer = commandLine.getOptionValue(opt, defaultValue);
      assert (answer != null);
      return answer;
  }

  /** 
   * Retrieve the argument, if any, of an option.
   *
   * @param opt character name of the option
   * @param defaultValue is the default value to be returned if the option 
   * is not specified
   * @return Value of the argument if option is set, and has an argument,
   * otherwise <code>defaultValue</code>.
   */
  public String getOptionValue(final char opt, final String defaultValue)
  {
    final String answer = commandLine.getOptionValue(opt, defaultValue);
    assert (answer != null);
    return answer;
  }

  /** 
   * Retrieve any left-over non-recognized options and arguments
   *
   * @return remaining items passed in but not parsed as an array
   */
  public String[] getArgs()
  {
      final String[] answer = commandLine.getArgs();
      /*
       * By implementation.
       */
      assert (answer != null);
      return answer;
  }

  /** 
   * Retrieve any left-over non-recognized options and arguments
   *
   * @return remaining items passed in but not parsed as a <code>List</code>.
   */
  public List<String> getArgList()
  {
      final List<String> answer = commandLine.getArgList();
      /*
       * By implementation
       */
      assert (answer != null);
      return answer;
  }

  /**
   * Returns an iterator over the Option members of CommandLine.
   *
   * @return an <code>Iterator</code> over the processed {@link Option} 
   * members of this {@link CommandLine}
   */
  public Iterator<Option> iterator()
  {
      final Iterator<Option> answer = commandLine.iterator();
      /*
       * Because of where answers are added, each element of the iterator
       * is also non-null.
       */
      assert (answer != null);
      return answer;
  }

  /**
   * Returns an array of the processed {@link Option}s.
   *
   * @return an array of the processed {@link Option}s.
   */
  public Option[] getOptions()
  {
    final Option[] answer = commandLine.getOptions();
    /*
     * By implementation.
     */
    assert (answer != null);
    return answer;
  }
}
