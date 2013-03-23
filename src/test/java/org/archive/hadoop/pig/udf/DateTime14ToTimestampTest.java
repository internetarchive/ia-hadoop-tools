/**
 * 
 */
package org.archive.hadoop.pig.udf;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;

import junit.framework.Assert;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.archive.hadoop.pig.udf.DateTime14ToTimestamp;
import org.junit.Test;


/**
 * @author kenji
 *
 */
public class DateTime14ToTimestampTest extends Assert {
  protected Tuple newTuple(Object arg1) {
    return TupleFactory.getInstance().newTuple(Collections.singletonList(arg1));
  }
  @Test
  public void testReturnType() {
    EvalFunc<Long> t = new DateTime14ToTimestamp();
    assertEquals(Long.class, t.getReturnType());
  }
  @Test
  public void testRegular14() throws IOException {
    EvalFunc<Long> t = new DateTime14ToTimestamp();
    Date d = new Date();
    Long ts = d.getTime() / 1000 * 1000;
    DateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
    String input = df.format(d);
    assertEquals(ts, t.exec(newTuple(input)));
  }
  @Test
  public void testRegular17() throws IOException {
    EvalFunc<Long> t = new DateTime14ToTimestamp();
    Date d = new Date();
    Long ts = d.getTime();
    DateFormat df = new SimpleDateFormat("yyyyMMddHHmmssSSS");
    String input = df.format(d);
    assertEquals(ts, t.exec(newTuple(input)));
  }
  @Test
  public void testNullForNull() throws IOException {
    EvalFunc<Long> t = new DateTime14ToTimestamp();
    assertNull("null for null input", t.exec(newTuple(null)));
  }
}
