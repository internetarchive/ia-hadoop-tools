/**
 * 
 */
package org.archive.modules.recrawl;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import org.archive.modules.hq.recrawl.HBaseClient;

/**
 * read crawl.log and store recrawl data into Hbase
 * @author kenji
 *
 */
public class StoreCrawlinfo {

  HBaseClient hbase;
  DateFormat isoDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss.SSS'Z'");
  
  public StoreCrawlinfo() {
    Date date = new Date();
    Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    isoDateFormat.format(date);
  }
  
  /**
   * @param args
   */
  public static void main(String[] args) {
    // TODO Auto-generated method stub
  }

}
