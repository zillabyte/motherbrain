package grandmotherbrain.utils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.joda.time.Period;

public class DateHelper {

  
  public final static long ONE_SECOND = 1000;
  public final static long SECONDS = 60;

  public final static long ONE_MINUTE = ONE_SECOND * 60;
  public final static long MINUTES = 60;

  public final static long ONE_HOUR = ONE_MINUTE * 60;
  public final static long HOURS = 24;

  public final static long ONE_DAY = ONE_HOUR * 24;
  public final static long ONE_WEEK = ONE_DAY * 7;
  public final static long ONE_MONTH = ONE_DAY * 30;
  //"2013-12-02 23:29:21"
  private static final SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  
  public static long getLongMillis(String date){
    try {
      return dateTimeFormat.parse(date).getTime();
    } catch (ParseException e) {
      return 0;
    }
    
  }
  
  public static String reverseFormattedDate() {
    
    Calendar cal = Calendar.getInstance();
    cal.add(Calendar.DATE, 1);
    SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
    String formatted = format.format(cal.getTime());
    assert (formatted != null);
    return formatted;
    
  }
  
  
  public static String reverseFormattedDateWithMilliseconds() {
    
    Calendar cal = Calendar.getInstance();
    cal.add(Calendar.DATE, 1);
    SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmssSSS");
    String formatted = format.format(cal.getTime());
    assert (formatted != null);
    return formatted;
    
  }
  
  
  
  public static String reverseFormattedDate(long date) {
    SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
    String formatted = format.format(Long.valueOf(date));
    assert (formatted != null);
    return formatted;
    
  }
  
  
  // 2013-07-14T23:46:29+00:00"
  // yyyy-MM-dd
  private final static Pattern rubyDatePattern;
  static {
    final Pattern maybePattern = Pattern.compile("(?:\\d{4})\\-(?:\\d{2})\\-(?:\\d{2})"); 
    assert(maybePattern != null);
    rubyDatePattern = maybePattern;
  }

  
  public static long parseRubyDate(String s) throws ParseException {
    Matcher m = rubyDatePattern.matcher(s);
    return rubyDateFormat.parse(m.group()).getTime();
  }
  
//  public static long maybeParseRubyDate(String s) {
//    try {
//      return parseRubyDate(s);
//    } catch (ParseException e) {
//      e.printStackTrace();
//      return def;
//    }
//  }

  
  public static long now() {
    return System.currentTimeMillis();
  }
  
  
  public static long monthAgo(int i) {
    return now() - (ONE_MONTH * i);
  }

  public static Date parseEpoch(long date) {
    return new Date(date);
  }
  
  private static SimpleDateFormat rubyDateFormat = new SimpleDateFormat("yyyy-MM-dd");
  public static String formatEpoch(long date) {
    final String parsedDate = rubyDateFormat.format(parseEpoch(date));
    assert(parsedDate != null);
    return parsedDate;
  }

  public static String reverseFormattedDate(Date date) {
    return reverseFormattedDate(date.getTime());
  }
  
  public static long roundToMidnight(long date) {
    return date - (date % ONE_DAY);
  }
  
  public static long roundToWeek(long date) {
    return date - (date % ONE_WEEK);
  }

  public static long roundToMonth(long date) {
      return date - (date % ONE_MONTH);
    }


  public static long roundToMidnight(Date extractDate) {
    return roundToMidnight(extractDate.getTime());
  }
  
  public static long roundToWeek(Date extractDate) {
    return roundToWeek(extractDate.getTime());
  }

  
  public static Date midnight() {
    return new Date(roundToMidnight(now())); 
  }

  
  
  public static final DateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  static {
    timestampFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
  }
  
  public static Date parseTimestamp(String string) throws ParseException {
    Date d = timestampFormat.parse(string.trim());
    return d;
  }
  
  public static String toTimestamp(Date d) {
    return timestampFormat.format(d);
  }

  public static String formattedDate() {
    return toTimestamp(new Date());
  }

  
  

  private final static Pattern _minutesPattern = Pattern.compile("(\\d+)\\s*(m|min|mins)");
  private final static Pattern _secondsPattern = Pattern.compile("(\\d+)\\s*(s|sec|secs)");
  private final static Pattern _millisPattern = Pattern.compile("(\\d+)\\s*(ms)");
  private final static Pattern _hoursPattern = Pattern.compile("(\\d+)\\s*(h|hours)");

  private static Integer _getDuration(Pattern p, String s) {
    Matcher m = p.matcher(s);
    if (m != null && m.find()) {
      return Integer.parseInt(m.group(1));
    } else {
      return null;
    }
  }
  
  public static Long parseDuration(String string) {
    Period p = Period.ZERO;
    if (_getDuration(_hoursPattern, string) != null) {
      p = p.withHours(_getDuration(_hoursPattern, string));
    }
    if (_getDuration(_minutesPattern, string) != null) {
      p = p.withMinutes(_getDuration(_minutesPattern, string));
    }
    if (_getDuration(_secondsPattern, string) != null) {
      p = p.withSeconds(_getDuration(_secondsPattern, string));
    }
    if (_getDuration(_millisPattern, string) != null) {
      p = p.withMillis(_getDuration(_millisPattern, string));
    }
    return p.toStandardDuration().getMillis();
  }
  
  
  
}
