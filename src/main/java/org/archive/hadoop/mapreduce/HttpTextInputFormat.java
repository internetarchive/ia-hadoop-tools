/**
 * 
 */
package org.archive.hadoop.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

/**
 * TextInputFormat that reads text data from HTTP URLs.
 * @author kenji
 *
 */
public class HttpTextInputFormat extends TextInputFormat {
  @Override
  public RecordReader<LongWritable, Text> createRecordReader(InputSplit split,
      TaskAttemptContext context) {
    return new HttpLineRecordReader();
  }
  
  // HTTP resources are not splittable for now.
  // actually no need to override this method as getSplits(JobContext) is also
  // overridden.
  @Override
  protected boolean isSplitable(JobContext context, Path file) {
    return false;
  }
  
  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    List<InputSplit> splits = new ArrayList<InputSplit>();
    Path[] uris = getInputPaths(job);
    for (Path uri : uris) {
      FileSplit split = new FileSplit(uri, 0, Long.MAX_VALUE, null);
      splits.add(split);
    }
    return splits;
  }
}
