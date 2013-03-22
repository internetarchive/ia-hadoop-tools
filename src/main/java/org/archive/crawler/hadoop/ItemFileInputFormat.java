/**
 * 
 */
package org.archive.crawler.hadoop;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * @author kenji
 *
 */
public abstract class ItemFileInputFormat<K, V> extends InputFormat<K, V> {
  private String itemid;
  private String filename;
  public ItemFileInputFormat(String itemid, String filename) {
    
  }
  
  @Override
  public RecordReader<K, V> createRecordReader(InputSplit arg0,
      TaskAttemptContext arg1) throws IOException, InterruptedException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<InputSplit> getSplits(JobContext arg0) throws IOException,
      InterruptedException {
    // TODO Auto-generated method stub
    return null;
  }

}
