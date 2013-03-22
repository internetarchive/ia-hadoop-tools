/**
 * 
 */
package org.archive.crawler.hadoop;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;

/**
 * Reads Line as record via HTTP, instead of from HDFS.
 * Large part of code was stolen from LineRecrodReader.
 * Currently there's no support for compression.
 * @see org.apache.hadoop.mapreduce.lib.input.LineRecordReader
 * @author Kenji Nagahashi
 *
 */
public class HttpLineRecordReader extends RecordReader<LongWritable, Text> {
  private static final Log LOG = LogFactory.getLog(HttpLineRecordReader.class);
  
  private CompressionCodecFactory compressionCodecs = null;
  private long start;
  private long pos;
  private long end;
  private LineReader in;
  private int maxLineLength;
  private LongWritable key = null;
  private Text value = null;
  
  public void initialize(InputSplit genericSplit,
      TaskAttemptContext context) throws IOException {
    FileSplit split = (FileSplit) genericSplit;
    Configuration job = context.getConfiguration();
    this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength",
	Integer.MAX_VALUE);
    start = split.getStart();
    end = start + split.getLength();
    final Path file = split.getPath();
    compressionCodecs = new CompressionCodecFactory(job);
    final CompressionCodec codec = compressionCodecs.getCodec(file);

    HttpClient httpClient = new DefaultHttpClient();
    HttpGet get = new HttpGet(file.toUri());
    // TODO: add headers to HttpGet. it is critical for accessing resource that requires
    // authentication/authorization. Also leverage Range request if available. 
    HttpResponse resp = httpClient.execute(get);
    // TODO: check status code.
    HttpEntity entity = resp.getEntity();
    InputStream fileIn = entity.getContent(); 
    // TODO: it is an interesting option to implement FileSystem for Petabox. Then
    // there'd be no need to re-implement InputFormat, RecordReader, LoadFunc, etc.
    // open the file and seek to the start of the split
//    FileSystem fs = file.getFileSystem(job);
//    FSDataInputStream fileIn = fs.open(split.getPath());
    boolean skipFirstLine = false;
    if (codec != null) {
      in = new LineReader(codec.createInputStream(fileIn), job);
      end = Long.MAX_VALUE;
    } else {
      if (start != 0) {
	skipFirstLine = true;
	--start;
//	fileIn.seek(start);
	// XXX - use Range request.
	long remBytes = start;
	byte[] buffer = new byte[1024];
	while (remBytes > 0) {
	  int n = fileIn.read(buffer, 0, (int)Math.min(buffer.length, remBytes));
	  if (n == -1) break;
	  remBytes -= n;
	}
      }
      in = new LineReader(fileIn, job);
    }
    if (skipFirstLine) {  // skip first line and re-establish "start".
      start += in.readLine(new Text(), 0,
	  (int)Math.min((long)Integer.MAX_VALUE, end - start));
    }
    this.pos = start;
  }

  public boolean nextKeyValue() throws IOException {
    if (key == null) {
      key = new LongWritable();
    }
    key.set(pos);
    if (value == null) {
      value = new Text();
    }
    int newSize = 0;
    while (pos < end) {
      newSize = in.readLine(value, maxLineLength,
	  Math.max((int)Math.min(Integer.MAX_VALUE, end-pos),
	      maxLineLength));
      if (newSize == 0) {
	break;
      }
      pos += newSize;
      if (newSize < maxLineLength) {
	break;
      }

      // line too long. try again
      LOG.info("Skipped line of size " + newSize + " at pos " + 
	  (pos - newSize));
    }
    if (newSize == 0) {
      key = null;
      value = null;
      return false;
    } else {
      return true;
    }
  }

  @Override
  public LongWritable getCurrentKey() {
    return key;
  }

  @Override
  public Text getCurrentValue() {
    return value;
  }

  /**
   * Get the progress within the split
   */
  public float getProgress() {
    if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (pos - start) / (float)(end - start));
    }
  }

  public synchronized void close() throws IOException {
    if (in != null) {
      in.close(); 
    }
  }
}
