/**
 * 
 */
package org.archive.hadoop.pig;

import java.io.IOException;

import junit.framework.Assert;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.archive.hadoop.mapreduce.LFOnlyLineRecordReader;
import org.archive.hadoop.pig.CDXLoader;
import org.junit.Test;

/**
 * @author kenji
 *
 */
public class CDXLoaderTest extends Assert {
  public static class FixtureLineRecordReader extends LFOnlyLineRecordReader {
    String[] lines;
    int pos;
    public FixtureLineRecordReader(String[] lines) {
      this.lines = lines;
      this.pos = -1;
    }
    @Override
    public boolean nextKeyValue() throws IOException {
      pos++;
      return pos < lines.length;
    }
    @Override
    public Text getCurrentValue() {
      if (pos >= lines.length)
	return null;
      Text value = new Text(lines[pos]);
      return value;
    }
  }
  /*
   * test for catching common error to forget aligning type cast in prepareToRead
   * with InputFormat class.  
   */
  @Test
  public void testPrepareToRead() throws IOException, InterruptedException {
    CDXLoader t = new CDXLoader();
    // most arguments can be null as they are not used in CDXLoader#prepareToRead()
    InputFormat<?, ?> fmt = t.getInputFormat();
    t.prepareToRead(fmt.createRecordReader(null, null), null);
  }
  @Test
  public void testRegularLine() throws IOException {
    CDXLoader t = new CDXLoader();
    String[] lines = {
	" CDX N b a m s k r M S V g",
	"101,78,123,109)/robots.txt 20120103084508 http://109.123.78.101/robots.txt text/html 404 22RZA2NQT3RZUCQYJYZDPVZRNYIR72SN - - 561 55426267 WIDE-20120103083324-crawl410/WIDE-20120103083324-00000.warc.gz"
    };
    t.prepareToRead(new FixtureLineRecordReader(lines), null);
    Tuple tuple = t.getNext();
    // CDXLoader should skip the header line. 
    //System.err.println(tuple.get(0).getClass());
    assertEquals(new DataByteArray("101,78,123,109)/robots.txt"), tuple.get(0));
    assertEquals(new DataByteArray("20120103084508"), tuple.get(1));
    assertEquals(new DataByteArray("http://109.123.78.101/robots.txt"), tuple.get(2));
    assertEquals(new DataByteArray("text/html"), tuple.get(3));
    assertEquals(new DataByteArray("404"), tuple.get(4));
    assertEquals(new DataByteArray("22RZA2NQT3RZUCQYJYZDPVZRNYIR72SN"), tuple.get(5));
    assertEquals(null, tuple.get(6));
    assertEquals(null, tuple.get(7));
    assertEquals(new DataByteArray("561"), tuple.get(8));
    assertEquals(new DataByteArray("55426267"), tuple.get(9));
    assertEquals(new DataByteArray("WIDE-20120103083324-crawl410/WIDE-20120103083324-00000.warc.gz"), tuple.get(10));
  }
  /*
   * Raw space in redirect URL. Current CDX generator writes out whatever text found in Location header
   * and meta-refresh, with absolutely no escaping. Spaces in redirect URL is fairly common.
   */
  @Test
  public void testSpaceInRedirect() throws IOException {
    CDXLoader t = new CDXLoader();
    String[] lines = {
	" CDX N b a m s k r M S V g",
	"131,125,136,219)/ 20120103084049 http://219.136.125.131/ text/html 302 3I42H3S6NNFQ2MSVX7XZKYAYSCX5QBYJ http://219.136.125.131/selfLogon.do?hoge=1 2 - 449 34133512 WIDE-20120103083324-crawl410/WIDE-20120103083324-00000.warc.gz"
    };
    t.prepareToRead(new FixtureLineRecordReader(lines), null);
    Tuple tuple = t.getNext();
    // CDXLoader should skip the header line. 
    //System.err.println(tuple.get(0).getClass());
    assertEquals(11, tuple.size());
    assertEquals(new DataByteArray("http://219.136.125.131/selfLogon.do?hoge=1 2"), tuple.get(6));
  }
  /*
   * CR in redirect URL. this problem is frequently found in redirect URLs coming from meta-refresh.
   */
  @Test
  public void testCRInRedirect() throws IOException {
    CDXLoader t = new CDXLoader();
    String[] lines = {
	" CDX N b a m s k r M S V g",
	"de,nuernberger)/rente 20110129033909 http://www.nuernberger.de/rente/ text/html 200 COWKAOTVKBT6YVW26BLW7T235FAWRSUR http://www.nuernberger.de/produkte/vorsorge_fuer_jung___alt/rente/\r - 581 468301945 COM-20110129023303-crawl306/COM-20110129025311-00174.warc.gz"
    };
    t.prepareToRead(new FixtureLineRecordReader(lines), null);
    Tuple tuple = t.getNext();
    // CDXLoader should skip the header line. 
    //System.err.println(tuple.get(0).getClass());
    assertEquals(11, tuple.size());
    assertEquals(new DataByteArray("http://www.nuernberger.de/produkte/vorsorge_fuer_jung___alt/rente/\r"), tuple.get(6));
  }
}
