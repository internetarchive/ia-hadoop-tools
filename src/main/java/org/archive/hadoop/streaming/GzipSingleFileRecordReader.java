package org.archive.hadoop.streaming;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.lib.CombineFileSplit;
import org.archive.util.zip.OpenJDK7GZIPInputStream;

public class GzipSingleFileRecordReader implements RecordReader<Text, Text>
{
	final static int MAX_ALLOW_FAILURES = 2;
	
	LineRecordReader reader;
	LongWritable longKey;
	String currPath;
	int currAttempt;
	
	public GzipSingleFileRecordReader(CombineFileSplit split, Configuration conf, Reporter reporter, Integer index) throws IOException
	{
		this(split.getPath(index), split.getOffset(index), conf);
	}
	
	public GzipSingleFileRecordReader(Path file, long startOffset, Configuration conf) throws IOException
	{
		currPath = file.toString();
		
		try {
			currAttempt = TaskAttemptID.forName(conf.get("mapred.task.id")).getId();
		
			FileSystem fs = file.getFileSystem(conf);

			FSDataInputStream fileIn = fs.open(file);
						
		    String delimiter = conf.get("textinputformat.record.delimiter");
		    
		    byte[] recordDelimiter = null;
		    
		    if (null != delimiter) {
		    	recordDelimiter = delimiter.getBytes();
		    }
		    
		    InputStream in = new OpenJDK7GZIPInputStream(fileIn);
		    
		    long endOffset = Long.MAX_VALUE;
		    
			reader = new LineRecordReader(in, startOffset, endOffset, conf, recordDelimiter);
			longKey = reader.createKey();
		} catch (Exception e) {
			if (currAttempt < MAX_ALLOW_FAILURES) {
				throw new IOException("Failed init for: " + currPath, e);
			}
		}		
	}

	@Override
	public boolean next(Text key, Text value) throws IOException {
		
		if (reader == null) {
			return false;
		}
		
		try {
			value.clear();
			return reader.next(longKey, key);
		} catch (Exception e) {
			if (currAttempt < MAX_ALLOW_FAILURES) {
				throw new IOException("Failed reading from " + currPath, e);	
			} else {
				key.clear();
				return false;
			}
		}
	}

	@Override
	public Text createKey() {
		return new Text();
	}

	@Override
	public Text createValue() {
		if (reader == null) {
			return new Text();
		}
		return reader.createValue();
	}

	@Override
	public long getPos() throws IOException {
		if (reader == null) {
			return 0;
		}
		
		return reader.getPos();
	}

	@Override
	public void close() throws IOException {
		if (reader != null) {
			reader.close();
		}
	}

	@Override
	public float getProgress() throws IOException {
		if (reader == null) {
			return 0;
		}
		return reader.getProgress();
	}
}