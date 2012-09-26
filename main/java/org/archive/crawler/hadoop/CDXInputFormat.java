/**
 * 
 */
package org.archive.crawler.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

/**
 * InputFormat for reading CDXes.
 * It is a {@link TextInputFormat} with just one modification: uses LFOnlyLineRecordReader
 * instead of standard LineRecordReader.
 * 
 * We may see more modifications that facilitates processing CDXes with Hadoop.
 * 
 * @author kenji
 *
 */
public class CDXInputFormat extends TextInputFormat {
  @Override
  public RecordReader<LongWritable, Text> createRecordReader(InputSplit split,
      TaskAttemptContext context) {
    return new LFOnlyLineRecordReader();
  }
}
