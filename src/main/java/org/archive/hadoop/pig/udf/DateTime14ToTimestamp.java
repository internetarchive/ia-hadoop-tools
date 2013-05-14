/**
 * 
 */
package org.archive.hadoop.pig.udf;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

/**
 * A PIG function that converts yyyyMMddHHmmss[SSS] format string (length 14 or 17) to long timestamp
 * value, milliseconds from the epoch. It is assumed that input string represents time in UTC.
 *  
 * @author kenji
 */
public class DateTime14ToTimestamp extends EvalFunc<Long> {
  @Override
  public Long exec(Tuple input) throws IOException {
    if (input.size() != 1)
      throw new IllegalArgumentException("expects just one chararray argument");
    Object arg = input.get(0);
    if (arg == null) return null;
    String dt = arg.toString();
    DateFormat df;
    if (dt.length() == 14) {
      df = new SimpleDateFormat("yyyyMMddHHmmss");
    } else if (dt.length() == 17) {
      df = new SimpleDateFormat("yyyyMMddHHmmssSSS");
    } else {
      throw new IllegalArgumentException("expects String of length 14 or 17 (got \"" + dt + "\")");
    }
    try {
      Date date = df.parse(dt);
      return date.getTime();
    } catch (ParseException ex) {
      throw new IllegalArgumentException("illegal format", ex);
    }
  }
}
