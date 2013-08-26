/**
 * 
 */
package org.archive.hadoop.pig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

/**
 * @author kenji
 *
 */
public class CrawlLogLoader extends LoadFunc {
    private static final Log log = LogFactory.getLog(CrawlLogLoader.class);
    
    private LineRecordReader in = null;

    private PigSplit split;
  
  public Tuple getNext() throws IOException {
    TupleFactory tupleFactory = TupleFactory.getInstance();
    String line;
    while (in.nextKeyValue()) {
      Text val = in.getCurrentValue();
      line = val.toString();
      if (line.length() > 0 && line.charAt(line.length() - 1) == '\r') {
	line = line.substring(0, line.length() - 1);
      }
      // 0: timestamp (YYYY-mm-ddTHH:MM:SSZ)
      // 1: status
      // 2: size (digits or "-")
      // 3: URI
      // 4: path
      // 5: via URI
      // 6: content-type
      // 7: thread (#\d+)
      // 8: starttime+duration, or "-" for exclusion case (status < 0) 
      // 9: content hash (sha1:BASE32)
      // 10: -
      // 11: -
      String[] fields = line.split("\\s+");
      if (fields.length >= 12) {
	List<DataByteArray> list = new ArrayList<DataByteArray>();
	for (int i = 0; i < fields.length; i++) {
	  if (i == 2) {
	    if (!Pattern.matches("\\d+$", fields[i]))
	      fields[i] = "-1";
	    list.add(new DataByteArray(fields[i]));
	  } else if (i == 8) {
	    String[] startDuration = fields[i].split("\\+", 2);
	    if (startDuration.length == 1) {
	        // column 8 is "-" for crawl exclusion (ex. by robots.txt)
	        if (startDuration[0].equals("-")) {
	            list.add(null);
	            list.add(null);
	        } else {
	            // abnormal case (no "+") - should never happen
	            list.add(new DataByteArray(startDuration[0]));
	            list.add(null);
	            log.warn("unexpected value (no '+') \"" + fields[i]
	                    + "\" in column 8 of line \"" + line
	                    + "\", split " + split);
	        }
	    } else {
	      // normal case
	      list.add(new DataByteArray(startDuration[0]));
	      list.add(new DataByteArray(startDuration[1]));
	      if (startDuration[0].length() == 0 || startDuration[1].length() == 0) {
	          log.warn("unexpected value (empty sub-field) \""
	                  + fields[i]
	                  + "\" in column 8 of line \""
	                  + line
	                  + "\", split " + split);
	      }
	    }
	  } else {
	    list.add(new DataByteArray(fields[i]));
	  }
	}
	return tupleFactory.newTuple(list);
      }
    }
    return null;
  }

  @Override
  public InputFormat<LongWritable, Text> getInputFormat() throws IOException {
    return new TextInputFormat();
  }

  @SuppressWarnings("unchecked")
  @Override
  public void prepareToRead(RecordReader reader, PigSplit split)
      throws IOException {
    in = (LineRecordReader)reader;
    this.split = split;
  }

  @Override
  public void setLocation(String location, Job job) throws IOException {
    FileInputFormat.setInputPaths(job, location);
  }
}
